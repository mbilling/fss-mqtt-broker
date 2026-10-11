//! Counting a message once (ADR 0085 D8): `kv_incr` with an idempotency key, on one
//! node. The namespace remembers the key for its dedupe window as an entry of its own,
//! and an increment whose key it remembers adds nothing.

use std::sync::Arc;

use mqtt_rules::state::{NamespaceSettings, Remembered, StateNamespace, StateOp, StateStore};

use super::{publish, rig, rules, Did, Rig, Store, ENTRY_OVERHEAD};
use crate::rule_state::{Limits, Selector};

/// What a remembered idempotency key of `len` bytes is accounted as: an entry, its
/// key (the marker byte and the idempotency key) and the time it was first seen.
fn remembered_bytes(len: usize) -> u64 {
    (ENTRY_OVERHEAD + 1 + len + 8) as u64
}

impl Rig {
    /// `kv_incr('<key>', 1, 3600, '<once>')` on this rig's `rule:test` namespace.
    fn count_once(&self, key: &str, once: &str) -> String {
        self.select(&format!("kv_incr('{key}', 1, 3600, '{once}') AS n"))
    }
}

/// One `kv_incr` with an idempotency key, on a namespace handle.
fn incr_once(ns: &dyn StateNamespace, key: &str, once: &str) -> Result<String, String> {
    ns.call(&StateOp::IncrOnce {
        key: key.as_bytes(),
        by: 1,
        ttl: None,
        once: once.as_bytes(),
    })
    .map(|reply| reply.value.unwrap().to_json().unwrap())
    .map_err(|e| e.to_string())
}

/// D8: the first sight of an idempotency key adds and remembers it; a later one within
/// the namespace's dedupe window changes nothing and answers the count as it is. The
/// window runs from the first sight and a resend does not move it.
#[test]
fn a_message_seen_again_within_the_window_is_counted_once() {
    let r = rig();
    assert_eq!(r.count_once("n", "m-1"), r#"{"n":1}"#);
    assert_eq!(r.count_once("n", "m-1"), r#"{"n":1}"#);
    assert_eq!(r.count_once("n", "m-2"), r#"{"n":2}"#);
    assert_eq!(r.count_once("n", "m-1"), r#"{"n":2}"#);
    // The step is the call's, and a resend takes none.
    assert_eq!(
        r.select("kv_incr('n', 10, 3600, 'm-3') AS n"),
        r#"{"n":12}"#
    );
    assert_eq!(
        r.select("kv_incr('n', 10, 3600, 'm-3') AS n"),
        r#"{"n":12}"#
    );
    // Without an idempotency key every evaluation counts, as before.
    assert_eq!(r.select("kv_incr('n') AS n"), r#"{"n":13}"#);
    assert_eq!(r.select("kv_incr('n') AS n"), r#"{"n":14}"#);
    // The counter, and one entry for each of the three keys.
    let stats = r.stats("rule:test");
    assert_eq!(stats.keys, 4);
    assert_eq!(
        stats.bytes,
        (ENTRY_OVERHEAD + 1 + 8) as u64 + 3 * remembered_bytes(3)
    );
    // The default window is five minutes, from the first sight: a resend 200 seconds
    // in does not start it again.
    r.advance_secs(200);
    assert_eq!(r.count_once("n", "m-1"), r#"{"n":14}"#);
    r.advance_secs(99);
    assert_eq!(r.count_once("n", "m-1"), r#"{"n":14}"#);
    r.advance_secs(1);
    assert_eq!(r.count_once("n", "m-1"), r#"{"n":15}"#);
    assert_eq!(r.count_once("n", "m-1"), r#"{"n":15}"#);
}

/// D8: an idempotency key is new or not **to the namespace**, whichever counter the
/// call names, and `dedupe_window` says for how long.
#[test]
fn an_idempotency_key_is_the_namespaces_and_lasts_its_dedupe_window() {
    let r = rig();
    let set = r.load(&rules(
        "[state.short]\ndedupe_window = \"10s\"\n[state.other]\n",
        &[
            (
                "a",
                Some("short"),
                "SELECT kv_incr('all', 1, 60, payload.id) AS all, \
                 kv_incr('mine', 1, 60, payload.id) AS mine, \
                 kv_incr('mine', 1, 60, 'mine:' + payload.id) AS named FROM \"t/#\"",
            ),
            (
                "b",
                Some("other"),
                "SELECT kv_incr('all', 1, 60, payload.id) AS all FROM \"t/#\"",
            ),
        ],
    ));
    let send = |id: &str| {
        let did = publish(&set, "t/1", &format!("{{\"id\":\"{id}\"}}"));
        did.into_iter().map(|(_, did)| did).collect::<Vec<_>>()
    };
    let out = |text: &str| Did::Out(vec![text.to_string()]);
    // The second counter of one rule is given a key the first has just used: it does
    // not count. A key that names its counter does. Another namespace has its own.
    assert_eq!(
        send("m-1"),
        [out(r#"{"all":1,"mine":0,"named":1}"#), out(r#"{"all":1}"#)]
    );
    assert_eq!(
        send("m-1"),
        [out(r#"{"all":1,"mine":1,"named":1}"#), out(r#"{"all":1}"#)]
    );
    r.advance_secs(10);
    assert_eq!(
        send("m-1"),
        [out(r#"{"all":2,"mine":1,"named":2}"#), out(r#"{"all":1}"#)]
    );
}

/// D8's "on one node the check is exact": any number of connection tasks that present
/// one idempotency key at once make one increment between them.
#[test]
fn concurrent_callers_with_one_idempotency_key_count_once() {
    const THREADS: usize = 8;
    const MESSAGES: usize = 4_000;
    let store = Store::new(64 << 20);
    let ns = store
        .open("n", &NamespaceSettings::defaults(64 << 20))
        .unwrap();
    let start = std::sync::Barrier::new(THREADS);
    // Per thread: how often the count it read went back or passed the messages there
    // are, and how many of its devices' messages it was the first to present.
    let seen: Vec<(usize, usize)> = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let (ns, start) = (ns.clone(), &start);
                scope.spawn(move || {
                    start.wait();
                    let (mut last, mut wrong, mut firsts) = (0, 0, 0);
                    for i in 0..MESSAGES {
                        // Every thread presents every message, and a counter of its
                        // own device.
                        let once = format!("m-{i}");
                        let count: usize = incr_once(&*ns, "hot", &once).unwrap().parse().unwrap();
                        wrong += usize::from(!(last..=MESSAGES).contains(&count));
                        last = count;
                        let device = format!("d-{}", i % 16);
                        let once = format!("{device}:{i}");
                        firsts += usize::from(incr_once(&*ns, &device, &once).unwrap() != "0");
                    }
                    (wrong, firsts)
                })
            })
            .collect();
        threads.into_iter().map(|t| t.join().unwrap()).collect()
    });
    assert_eq!(seen.iter().map(|(wrong, _)| wrong).sum::<usize>(), 0);
    assert!(seen.iter().map(|(_, firsts)| firsts).sum::<usize>() >= MESSAGES);
    let read = |key: &str| {
        let reply = ns.call(&StateOp::Get {
            key: key.as_bytes(),
        });
        reply.unwrap().value.unwrap().to_json().unwrap()
    };
    assert_eq!(read("hot"), MESSAGES.to_string());
    let devices: usize = (0..16)
        .map(|d| read(&format!("d-{d}")).parse::<usize>().unwrap())
        .sum();
    assert_eq!(devices, MESSAGES);
    // 17 counters and two remembered keys a message, and the accounting is whole.
    let stats = store.stats("n").unwrap();
    assert_eq!(stats.keys, 17 + 2 * MESSAGES);
    assert_eq!(stats.bytes, store.bytes());
}

/// D9: a remembered key is a key of the namespace. A namespace with no room for it
/// refuses the increment with the error any new key gets, and nothing is counted; one
/// with room for it and none for a new counter refuses too and does not remember the
/// key, so the message is counted when it is sent again and there is room.
#[test]
fn a_full_namespace_refuses_the_increment_and_counts_the_message_when_there_is_room() {
    let r = rig();
    let set = r.load(&rules(
        "[state.small]\nmax_keys = 3\n",
        &[
            (
                "count",
                Some("small"),
                "SELECT kv_incr(payload.k, 1, 60, payload.id) AS n FROM \"in\"",
            ),
            (
                "put",
                Some("small"),
                "SELECT kv_set('x', 1) AS v FROM \"put\"",
            ),
            (
                "del",
                Some("small"),
                "SELECT kv_del(payload.k) AS v FROM \"del\"",
            ),
        ],
    ));
    let count = |k: &str, id: &str| {
        let payload = format!("{{\"k\":\"{k}\",\"id\":\"{id}\"}}");
        publish(&set, "in", &payload).remove(0).1
    };
    let counted = |n: u32| Did::Out(vec![format!("{{\"n\":{n}}}")]);
    let full = |did: Did| matches!(did, Did::Failed(e) if e.starts_with("kv_incr(): namespace_full: namespace `small` holds 3 keys"));
    assert_eq!(count("a", "m-1"), counted(1));
    assert_eq!(count("a", "m-2"), counted(2));
    // Three keys: the counter and two remembered ones. A third message is refused,
    // again and again, and never counted; the two that were counted still are.
    assert!(full(count("a", "m-3")));
    assert!(full(count("a", "m-3")));
    assert_eq!(count("a", "m-2"), counted(2));
    let stats = r.stats("small");
    assert_eq!((stats.keys, stats.refused_namespace_full), (3, 2));
    // Room for the remembered key and none for the counter it would count in: the
    // call is refused whole, and the key is not remembered.
    publish(&set, "del", r#"{"k":"a"}"#);
    publish(&set, "put", "{}");
    assert_eq!(r.stats("small").keys, 3);
    publish(&set, "del", r#"{"k":"x"}"#);
    publish(&set, "put", "{}");
    let held = |key: &[u8]| r.store.key("small", key).unwrap().is_some();
    assert!(held(Remembered::of(b"m-1").key()) && held(Remembered::of(b"m-2").key()));
    // (`x` and the two remembered keys; `a` is gone.)
    assert!(full(count("b", "m-4")));
    assert!(!held(Remembered::of(b"m-4").key()));
    // Past the window the two remembered keys are room: the message is counted now,
    // and only once.
    r.advance_secs(300);
    assert_eq!(count("b", "m-4"), counted(1));
    assert_eq!(count("b", "m-4"), counted(1));
    let stats = r.stats("small");
    assert_eq!(stats.keys, 3);
    assert_eq!(stats.bytes, r.store.bytes());
}

/// A call the counter's key refuses is not remembered either, so the rule's error is
/// what a resend gets too, and a duplicate reads `0` where the counter has gone.
#[test]
fn a_refused_increment_is_not_remembered_and_a_duplicate_reads_the_count_there_is() {
    let r = rig();
    assert_eq!(r.select("kv_set('s', 'text') AS v"), r#"{"v":"text"}"#);
    let wrong = "kv_incr(): wrong_type: the key holds a value of type `lww`, and this is an \
                 operation on type `sum` (kv_del deletes a key of any type)";
    assert_eq!(r.fails("kv_incr('s', 1, 60, 'm-1') AS n"), wrong);
    assert_eq!(r.fails("kv_incr('s', 1, 60, 'm-1') AS n"), wrong);
    assert_eq!(r.stats("rule:test").keys, 1);
    // The same message counts where it can.
    assert_eq!(r.select("kv_incr('n', 1, 60, 'm-1') AS n"), r#"{"n":1}"#);
    // A duplicate still checks what it would have counted in.
    assert_eq!(r.fails("kv_incr('s', 1, 60, 'm-1') AS n"), wrong);
    // A counter that lives for less than the window: a duplicate after it expired
    // reads 0 and makes no key.
    assert_eq!(r.select("kv_incr('brief', 1, 1, 'm-2') AS n"), r#"{"n":1}"#);
    r.advance_secs(2);
    assert_eq!(r.select("kv_incr('brief', 1, 1, 'm-2') AS n"), r#"{"n":0}"#);
    assert_eq!(r.select("kv_get('brief') AS v"), r#"{"v":"undefined"}"#);
    // An increment past 64 bits is refused, and its key is not remembered.
    assert_eq!(
        r.select("kv_incr('big', 9223372036854775807, 60, 'm-3') AS n"),
        r#"{"n":9223372036854775807}"#
    );
    assert_eq!(
        r.fails("kv_incr('big', 1, 60, 'm-4') AS n"),
        "kv_incr(): refused: the counter would pass 64 bits"
    );
    assert_eq!(r.select("kv_incr('n', 1, 60, 'm-4') AS n"), r#"{"n":2}"#);
}

/// D10: a remembered key is listed, read and deleted by an operator like any key of
/// the namespace. It is the key that starts with a NUL byte, of type `first`, holding
/// when it was first seen; a prefix of that byte lists exactly the remembered keys,
/// and no rule's prefix lists one. Deleting it lets the message count again.
#[test]
fn an_operator_sees_remembered_keys_as_keys_of_the_namespace() {
    let r = rig();
    assert_eq!(r.count_once("n", "m-1"), r#"{"n":1}"#);
    assert_eq!(r.count_once("n", "m-2"), r#"{"n":2}"#);
    let list = |prefix: &[u8]| {
        let page = r
            .store
            .keys(
                "rule:test",
                &Selector::prefix(prefix),
                None,
                &Limits::default(),
            )
            .unwrap();
        assert!(page.next.is_none());
        let mut keys: Vec<(Vec<u8>, &str)> = page
            .keys
            .iter()
            .map(|k| (k.key.to_vec(), k.type_name))
            .collect();
        keys.sort();
        keys
    };
    assert_eq!(
        list(b""),
        [
            (b"\0m-1".to_vec(), "first"),
            (b"\0m-2".to_vec(), "first"),
            (b"n".to_vec(), "sum")
        ]
    );
    assert_eq!(list(b"\0").len(), 2);
    assert_eq!(list(b"m").len(), 0);
    let seen = r.store.key("rule:test", b"\0m-1").unwrap().unwrap();
    assert_eq!(
        (
            seen.type_name,
            seen.value.to_json().unwrap(),
            seen.expires_ms
        ),
        ("first", "1800000000000".to_string(), 1_800_000_300_000)
    );
    assert_eq!(r.store.counts("rule:test").unwrap().keys, 3);
    assert!(r.store.delete_key("rule:test", b"\0m-1").unwrap());
    assert_eq!(r.count_once("n", "m-1"), r#"{"n":3}"#);
    assert_eq!(r.count_once("n", "m-2"), r#"{"n":3}"#);
}

/// D7 and D8 together: a remembered key that expires raises no `$events/state/expired`,
/// is not held for one, and does not take a place in the events' pace.
#[test]
fn a_remembered_key_expires_without_an_event() {
    let r = rig();
    r.store.set_expiry_events(true);
    for i in 0..200 {
        r.count_once("n", &format!("m-{i}"));
    }
    assert_eq!(r.select("kv_set('k', 1, 300) AS v"), r#"{"v":1}"#);
    assert_eq!(r.stats("rule:test").keys, 202);
    r.advance_secs(300);
    // A resend after the window finds its key expired: the key goes at once (it is
    // owed no event), and the message counts again.
    assert_eq!(r.count_once("n", "m-0"), r#"{"n":201}"#);
    assert_eq!(r.stats("rule:test").keys, 202);
    // A sweep that may hand over one event returns the one key that raises one. The
    // remembered keys it passes on the way are removed and take none of that
    // allowance; the rest go with the next pass, which has nothing to hand over.
    let swept = r.store.sweep(1);
    let expired: Vec<&[u8]> = swept.expired.iter().map(|e| &*e.key).collect();
    assert_eq!(expired, [b"k"]);
    let again = r.store.sweep(1);
    assert!(again.expired.is_empty() && !again.more);
    assert_eq!(swept.removed + again.removed, 199);
    // The counter (an hour to live) and the key seen again.
    let stats = r.stats("rule:test");
    assert_eq!(stats.keys, 2);
    assert_eq!(stats.bytes, r.store.bytes());
    // A replay asks for expired keys one at a time: a remembered one is taken out and
    // reports nothing.
    r.advance_secs(300);
    assert!(r.store.take_expired("rule:test", b"\0m-0").is_none());
    assert_eq!(r.stats("rule:test").keys, 1);
}

/// D9: in an `lru` namespace a remembered key is evicted like any key, the least
/// recently used first, so a full one remembers for less than its window.
#[test]
fn an_lru_namespace_evicts_remembered_keys_like_any_other() {
    let store = Store::new(64 << 20);
    let settings = NamespaceSettings {
        max_keys: 3,
        eviction: mqtt_rules::state::Eviction::Lru,
        ..NamespaceSettings::defaults(64 << 20)
    };
    let ns: Arc<dyn StateNamespace> = store.open("cache", &settings).unwrap();
    for (once, count) in [("m-1", "1"), ("m-2", "2"), ("m-2", "2"), ("m-1", "2")] {
        assert_eq!(incr_once(&*ns, "n", once).unwrap(), count);
    }
    // A third key evicts the least recently seen one, `m-2`, and that message is
    // then new again (which evicts `m-1`).
    assert_eq!(incr_once(&*ns, "n", "m-3").unwrap(), "3");
    assert_eq!(incr_once(&*ns, "n", "m-2").unwrap(), "4");
    assert_eq!(incr_once(&*ns, "n", "m-3").unwrap(), "4");
    let stats = store.stats("cache").unwrap();
    assert_eq!((stats.keys, stats.evicted), (3, 2));
}
