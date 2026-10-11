//! Counting a message once across nodes (ADR 0085 D8): the entry that remembers an
//! idempotency key is a key of the `eventual` namespace, so it is sent to every
//! member like any other. What that gives, and what it does not: a node that has
//! heard of the entry does not count the message again; two nodes that both see the
//! message before either has heard of the other's entry both count it.

use mqtt_rules::state::{NamespaceSettings, Remembered, StateOp};

use super::{eventual, node, node_with, Net, Node, NS};

impl Node {
    /// `kv_incr(key, 1, <no ttl>, once)`.
    fn incr_once(&self, key: &str, once: &str) -> Result<String, String> {
        self.call(&StateOp::IncrOnce {
            key: key.as_bytes(),
            by: 1,
            ttl: None,
            once: once.as_bytes(),
        })
    }

    /// When this copy says `once` was first seen, in milliseconds; `None` when it
    /// does not remember it.
    fn first_seen(&self, once: &str) -> Option<String> {
        let entry = Remembered::of(once.as_bytes());
        let seen = self.store.key(NS, entry.key()).unwrap()?;
        assert_eq!(seen.type_name, "first");
        Some(seen.value.to_json().unwrap())
    }
}

/// The acceptance's second half: a resend that reaches another node once the entry
/// has replicated is not counted, changes nothing there and sends nothing.
#[test]
fn a_message_counted_on_one_node_is_not_counted_again_where_the_entry_has_arrived() {
    let mut net = Net::new(vec![node("a"), node("b"), node("c")]);
    net.connect_all();
    net.settle();
    assert_eq!(net.nodes[0].incr_once("n", "m-1"), Ok("1".into()));
    net.settle();
    // The counter and the entry crossed to both of the others.
    assert_eq!(net.nodes[1].get("n"), "1");
    assert!(net.nodes[2].first_seen("m-1").is_some());
    let sent = net.sent;
    net.advance_ms(40);
    for n in 0..3 {
        assert_eq!(net.nodes[n].incr_once("n", "m-1"), Ok("1".into()));
    }
    net.settle();
    assert_eq!(net.sent, sent, "a resend that is not counted is not sent");
    // Another message counts wherever it arrives, once.
    assert_eq!(net.nodes[2].incr_once("n", "m-2"), Ok("2".into()));
    net.settle();
    assert_eq!(net.nodes[0].incr_once("n", "m-2"), Ok("2".into()));
    net.settle();
    net.assert_converged();
    assert_eq!(net.nodes[1].get("n"), "2");
    // The counter and two entries, on every node.
    for node in &net.nodes {
        assert_eq!(node.keys(), 3);
    }
}

/// What `eventual` does not give (D8: "a resend that reaches another node inside the
/// replication lag counts twice"): two nodes that see one message before either has
/// heard of the other's entry each count it, and the merge keeps both counts. The
/// two entries merge to the earlier, so every node then remembers the key alike, and
/// a third sight anywhere counts nothing.
#[test]
fn two_nodes_that_see_a_message_before_hearing_of_each_other_both_count_it() {
    let mut net = Net::new(vec![node("a"), node("b"), node("c")]);
    assert_eq!(net.nodes[0].incr_once("n", "m-1"), Ok("1".into()));
    net.nodes[1].advance_ms(7);
    assert_eq!(net.nodes[1].incr_once("n", "m-1"), Ok("1".into()));
    net.connect_all();
    net.repair();
    net.assert_converged();
    for node in &net.nodes {
        // One count from each node that saw it unheard: at most one a node.
        assert_eq!(node.get("n"), "2");
        // The earlier of the two sights, which was node a's.
        assert_eq!(node.first_seen("m-1"), Some(super::START_MS.to_string()));
        assert_eq!(node.incr_once("n", "m-1"), Ok("2".into()));
    }
    net.settle();
    net.assert_converged();
    assert_eq!(net.nodes[2].get("n"), "2");
}

/// The window is the same moment on every node: a key remembered for five minutes is
/// new again everywhere after them, by the clock the nodes keep between them.
#[test]
fn a_remembered_key_is_forgotten_on_every_node_when_its_window_is_over() {
    let short = NamespaceSettings {
        dedupe_window_secs: 30,
        ..eventual()
    };
    let mut net = Net::new(vec![
        node_with("a", &short, 64 << 20),
        node_with("b", &short, 64 << 20),
    ]);
    net.connect_all();
    net.settle();
    assert_eq!(net.nodes[0].incr_once("n", "m-1"), Ok("1".into()));
    net.settle();
    net.advance_ms(29_999);
    assert_eq!(net.nodes[1].incr_once("n", "m-1"), Ok("1".into()));
    net.advance_ms(1);
    assert_eq!(net.nodes[1].first_seen("m-1"), None);
    assert_eq!(net.nodes[1].incr_once("n", "m-1"), Ok("2".into()));
    net.settle();
    assert_eq!(net.nodes[0].incr_once("n", "m-1"), Ok("2".into()));
    net.settle();
    net.assert_converged();
}

/// An increment its counter refuses is not remembered here either: the entry is
/// deleted again, a delete like any other, so the message counts when it comes again.
/// And a namespace with no room for the entry refuses the increment whole.
#[test]
fn a_refused_increment_is_not_remembered_in_an_eventual_namespace() {
    let n = node("a");
    n.set("s", "text");
    assert_eq!(n.incr_once("s", "m-1"), Err("wrong_type".into()));
    assert_eq!(n.first_seen("m-1"), None);
    assert_eq!(n.incr_once("n", "m-1"), Ok("1".into()));
    assert_eq!(n.incr_once("n", "m-1"), Ok("1".into()));
    // A duplicate still checks the key it would have counted in, and reads 0 where
    // there is no counter.
    assert_eq!(n.incr_once("s", "m-1"), Err("wrong_type".into()));
    assert_eq!(n.incr_once("never", "m-1"), Ok("0".into()));
    assert_eq!(n.get("never"), "undefined");

    let small = NamespaceSettings {
        max_keys: 3,
        ..eventual()
    };
    let n = node_with("a", &small, 64 << 20);
    assert_eq!(n.incr_once("n", "m-1"), Ok("1".into()));
    assert_eq!(n.incr_once("n", "m-2"), Ok("2".into()));
    assert_eq!(n.incr_once("n", "m-3"), Err("namespace_full".into()));
    assert_eq!(n.incr_once("n", "m-3"), Err("namespace_full".into()));
    assert_eq!((n.get("n"), n.keys()), ("2".into(), 3));
    // Past the window the entries can be forgotten, and are room.
    n.advance_ms(3_601_000);
    assert_eq!(n.incr_once("n", "m-3"), Ok("3".into()));
    assert_eq!(n.incr_once("n", "m-3"), Ok("3".into()));
}

/// On one node of an `eventual` namespace the check is exact, as in a `local` one:
/// threads that present one idempotency key at once make one increment between them,
/// while a link reads what they write.
#[test]
fn concurrent_callers_of_an_eventual_namespace_with_one_idempotency_key_count_once() {
    const THREADS: usize = 8;
    const MESSAGES: usize = 2_000;
    let mut net = Net::new(vec![node("a"), node("b")]);
    net.connect_all();
    net.settle();
    let a = &net.nodes[0];
    let start = std::sync::Barrier::new(THREADS);
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                start.wait();
                for i in 0..MESSAGES {
                    a.incr_once("hot", &format!("m-{i}")).unwrap();
                }
            });
        }
    });
    assert_eq!(a.get("hot"), MESSAGES.to_string());
    assert_eq!(a.keys(), MESSAGES + 1);
    net.settle();
    net.assert_converged();
    assert_eq!(net.nodes[1].get("hot"), MESSAGES.to_string());
}
