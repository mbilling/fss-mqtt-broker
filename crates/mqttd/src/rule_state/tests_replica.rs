//! `eventual` namespaces (ADR 0085 D5, D15): several nodes' stores, and between them
//! the same [`StateLink`]s the peer links run, driven here by hand.
//!
//! Nothing below waits for time. Each node's wall clock is a number the test moves,
//! and the links are asked for their next frame at an instant the test names
//! ([`StateLink::poll`]), so a test decides what is delivered, when, how often and in
//! what order: a partition is links that do not exist, a slow link one nobody asks,
//! a restart a new store under an old name.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mqtt_cluster::peer::{PeerMessage, STATE_PARTS};
use mqtt_rules::crdt::stable_hash;
use mqtt_rules::state::{
    term_key, Consistency, NamespaceSettings, StateNamespace, StateOp, StateStore,
};
use mqtt_rules::{Functions, RuleSet, Value};

use super::replica::{Refused, Replication};
use super::{stored, StateLink, Store};

const NS: &str = "shared";

/// The moment every node's clock starts at.
const START_MS: u64 = 1_800_000_000_000;

fn eventual() -> NamespaceSettings {
    NamespaceSettings {
        consistency: Consistency::Eventual,
        ..NamespaceSettings::defaults(64 << 20)
    }
}

/// One node: its store, its clock, and the namespace as its rules would call it.
struct Node {
    id: String,
    store: Arc<Store>,
    clock: Arc<AtomicU64>,
    ns: Arc<dyn StateNamespace>,
}

fn node(id: &str) -> Node {
    node_with(id, &eventual(), 64 << 20)
}

fn node_with(id: &str, settings: &NamespaceSettings, store_max_bytes: u64) -> Node {
    let clock = Arc::new(AtomicU64::new(START_MS));
    let store = Store::with_manual_clock(store_max_bytes, clock.clone());
    store.set_node(id);
    let ns = store.open(NS, settings).unwrap();
    Node {
        id: id.to_string(),
        store,
        clock,
        ns,
    }
}

impl Node {
    fn call(&self, op: &StateOp<'_>) -> Result<String, String> {
        self.ns
            .call(op)
            .map(|reply| {
                reply
                    .value
                    .map_or_else(|| "undefined".to_string(), |value| value.to_json().unwrap())
            })
            .map_err(|e| e.reason.to_string())
    }

    fn get(&self, key: &str) -> String {
        self.call(&StateOp::Get {
            key: key.as_bytes(),
        })
        .unwrap()
    }

    fn set(&self, key: &str, value: &str) {
        let value = Value::from(value);
        self.call(&StateOp::Set {
            key: key.as_bytes(),
            value: stored(&value),
            ttl: None,
        })
        .unwrap();
    }

    fn set_for(&self, key: &str, value: &str, ttl: u64) {
        let value = Value::from(value);
        self.call(&StateOp::Set {
            key: key.as_bytes(),
            value: stored(&value),
            ttl: Some(ttl),
        })
        .unwrap();
    }

    fn incr(&self, key: &str, by: i64) -> String {
        self.call(&StateOp::Incr {
            key: key.as_bytes(),
            by,
            ttl: None,
        })
        .unwrap()
    }

    fn add(&self, key: &str, element: &str) -> Result<String, String> {
        let element = Value::from(element);
        self.call(&StateOp::Add {
            key: key.as_bytes(),
            element: stored(&element),
            identity: &term_key(&element),
            ttl: None,
        })
    }

    fn max(&self, key: &str, value: i64) -> String {
        let value = Value::Int(value);
        self.call(&StateOp::Extreme {
            key: key.as_bytes(),
            value: stored(&value),
            largest: true,
            ttl: None,
        })
        .unwrap()
    }

    fn set_once(&self, key: &str, value: &str) -> String {
        let value = Value::from(value);
        self.call(&StateOp::SetOnce {
            key: key.as_bytes(),
            value: stored(&value),
            ttl: None,
        })
        .unwrap()
    }

    fn hll_add(&self, key: &str, value: &str) -> String {
        self.call(&StateOp::HllAdd {
            key: key.as_bytes(),
            hash: stable_hash(&term_key(&Value::from(value))),
            ttl: None,
        })
        .unwrap()
    }

    fn del(&self, key: &str) -> String {
        self.call(&StateOp::Del {
            key: key.as_bytes(),
        })
        .unwrap()
    }

    fn advance_ms(&self, ms: u64) {
        self.clock.fetch_add(ms, Relaxed);
    }

    /// Every state this copy holds, encoded, in key order.
    fn snapshot(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let ns = super::lock(&self.store.namespaces)
            .get(NS)
            .and_then(std::sync::Weak::upgrade)
            .unwrap();
        ns.replica.as_ref().unwrap().snapshot(&ns)
    }

    fn keys(&self) -> usize {
        self.store.stats(NS).unwrap().keys
    }

    fn bytes(&self) -> u64 {
        self.store.stats(NS).unwrap().bytes
    }

    fn replication(&self) -> Replication {
        self.store.replication(NS).unwrap()
    }
}

/// What crossed the links.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Sent {
    /// Frames of any kind.
    frames: usize,
    /// Keys, with their states.
    keys: usize,
    /// Digests with their parts.
    digests: usize,
}

/// Nodes, and the links between them that the test has made.
struct Net {
    nodes: Vec<Node>,
    /// `(from, to)`: the link of node `from` to node `to`.
    links: BTreeMap<(usize, usize), StateLink>,
    now: Instant,
    sent: Sent,
}

impl Net {
    fn new(nodes: Vec<Node>) -> Self {
        Self {
            nodes,
            links: BTreeMap::new(),
            now: Instant::now(),
            sent: Sent::default(),
        }
    }

    /// A link comes up between nodes `a` and `b`.
    fn connect(&mut self, a: usize, b: usize) {
        for (from, to) in [(a, b), (b, a)] {
            let link = StateLink::new(
                self.nodes[from].store.clone(),
                &self.nodes[from].id,
                &self.nodes[to].id,
            );
            self.links.insert((from, to), link);
        }
    }

    fn disconnect(&mut self, a: usize, b: usize) {
        self.links.remove(&(a, b));
        self.links.remove(&(b, a));
    }

    fn connect_all(&mut self) {
        for a in 0..self.nodes.len() {
            for b in a + 1..self.nodes.len() {
                self.connect(a, b);
            }
        }
    }

    fn count(&mut self, frame: &PeerMessage) {
        self.sent.frames += 1;
        match frame {
            PeerMessage::StateKeys { entries, .. } => self.sent.keys += entries.len(),
            PeerMessage::StateDigest { parts, .. } if !parts.is_empty() => self.sent.digests += 1,
            _ => {}
        }
    }

    /// The next frame of the link `from` → `to`, if it has one now.
    fn take(&mut self, from: usize, to: usize) -> Option<PeerMessage> {
        let frame = self.links.get_mut(&(from, to))?.poll(self.now)?;
        self.count(&frame);
        Some(frame)
    }

    /// `frame`, sent on the link `from` → `to`, arrives.
    fn deliver(&mut self, from: usize, to: usize, frame: PeerMessage) {
        if let Some(link) = self.links.get_mut(&(to, from)) {
            link.receive_at(frame, self.now);
        }
    }

    /// Every link sends one frame if it has one, and it arrives. Whether any did.
    fn step(&mut self) -> bool {
        let mut any = false;
        let links: Vec<(usize, usize)> = self.links.keys().copied().collect();
        for (from, to) in links {
            if let Some(frame) = self.take(from, to) {
                self.deliver(from, to, frame);
                any = true;
            }
        }
        any
    }

    /// Run the links until none has anything to send, without the time for another
    /// comparison passing.
    fn settle(&mut self) {
        for _ in 0..1_000_000 {
            if !self.step() {
                // Past the pause between two rounds of changed keys.
                self.now += Duration::from_millis(10);
                if !self.step() {
                    return;
                }
            }
        }
        panic!("the links never ran out of frames to send");
    }

    /// The time for the next comparison passes, and the links run until quiet.
    fn repair(&mut self) {
        self.now += Duration::from_secs(31);
        self.settle();
    }

    fn advance_ms(&mut self, ms: u64) {
        for node in &self.nodes {
            node.advance_ms(ms);
        }
    }

    /// Every node holds the same states, byte for byte.
    fn assert_converged(&self) {
        let first = self.nodes[0].snapshot();
        for node in &self.nodes[1..] {
            assert!(
                node.snapshot() == first,
                "node {} differs from node {}",
                node.id,
                self.nodes[0].id
            );
        }
    }
}

/// A small generator, so that a failing schedule can be run again.
struct Rng(u64);

impl Rng {
    fn next(&mut self, below: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % below
    }

    fn byte(&mut self) -> u8 {
        u8::try_from(self.next(256)).unwrap()
    }
}

/// One message through `set`: what its rule printed, or why it failed.
fn publish(set: &RuleSet, topic: &str, payload: &str) -> String {
    let payload = bytes::Bytes::from(payload.to_string());
    let props = mqtt_core::AppProperties::default();
    let input = mqtt_rules::PublishInput::new("c1", topic, &payload, 1, &props);
    let mut said = Vec::new();
    let mut out = Vec::new();
    set.on_publish(
        &input,
        &mut |_, outcome| {
            if let mqtt_rules::Outcome::Failed(e) = outcome {
                said.push(format!("failed: {e}"));
            }
        },
        &mut out,
    );
    for (_, effect) in &out {
        if let mqtt_rules::Effect::Console(line) = effect {
            said.push(line.clone());
        }
    }
    said.join("\n")
}

// -- one node --------------------------------------------------------------------------

/// D5: on one node an `eventual` namespace answers a rule as a `local` one does. The
/// same statements through both, message by message.
#[test]
fn an_eventual_namespace_answers_a_rule_as_a_local_one_does() {
    let statements = [
        "kv_get('k') AS a, kv_set('k', payload.v) AS b, kv_get('k') AS c, kv_ttl('k') AS t",
        "kv_set('k', 'next', 60) AS a, kv_ttl('k') AS t, kv_del('k') AS d, kv_del('k') AS e, kv_get('k') AS g",
        "kv_del('never') AS a, kv_get('never') AS b, kv_ttl('never') AS c",
        "kv_set_once('o', 'first') AS a, kv_set_once('o', 'second') AS b, kv_get('o') AS g",
        "kv_incr('n') AS a, kv_incr('n', 5) AS b, kv_incr('n', -10) AS c, kv_get('n') AS g",
        "kv_max('hi', 3) AS a, kv_max('hi', 2) AS b, kv_max('hi', 7.5) AS c, kv_min('lo', 3) AS d, kv_min('lo', 9) AS e, kv_min('lo', -1) AS f",
        "kv_add('s', 'b') AS a, kv_add('s', 'a') AS b, kv_add('s', 'b') AS c, kv_add('s', 1) AS d, kv_get('s') AS g",
        "kv_hll_add('h', 'x') AS a, kv_hll_add('h', 'x') AS b, kv_hll_add('h', 'y') AS c, kv_hll_count('h') AS d, kv_hll_count('none') AS e, kv_get('h') AS g",
        "kv_held('hot', true) AS a, kv_held('hot', true, 600) AS b, kv_ttl('hot') AS t",
        "kv_held('hot', false) AS a, kv_get('hot') AS g, kv_held('hot', false) AS b",
        "kv_changed('st', 'on') AS a, kv_changed('st', 'on') AS b, kv_changed('st', 'off') AS c, kv_changed('st', 'off', 30) AS d, kv_ttl('st') AS t",
        "kv_set('big', 123456789012345678901234567890) AS a, kv_get('big') AS b, kv_set('m', payload) AS c, kv_get('m') AS d",
    ];
    let failing = [
        "kv_incr('k2') AS a, kv_set('k2', 1) AS b",
        "kv_set('k3', 1) AS a, kv_add('k3', 1) AS b",
        "kv_set_once('k4', 'x') AS a, kv_held('k4', true) AS b",
        "kv_add('k5', 1) AS a, kv_hll_count('k5') AS b",
        "kv_incr('k6', 9223372036854775807) AS a, kv_incr('k6') AS b",
    ];
    let run = |mode: &str| {
        let clock = Arc::new(AtomicU64::new(START_MS));
        let store = Store::with_manual_clock(64 << 20, clock.clone());
        store.set_node("node-1");
        let functions = Functions::default().with_state(store.clone());
        let mut file = format!("[state.default]\nconsistency = \"{mode}\"\n");
        for (i, fields) in statements.iter().chain(&failing).enumerate() {
            let _ = write!(
                file,
                "[rules.r{i}]\nstate = \"one\"\nsql = '''SELECT {fields} FROM \"t/{i}\"'''\n\
                 actions = [{{ function = \"console\" }}]\n"
            );
        }
        let _ = write!(file, "[state.one]\nconsistency = \"{mode}\"\n");
        let set = RuleSet::parse_with(&file, &functions).unwrap().rules;
        let mut said = Vec::new();
        for i in 0..statements.len() + failing.len() {
            said.push(publish(
                &set,
                &format!("t/{i}"),
                r#"{"v":{"a":[1,2.5,"x",null]}}"#,
            ));
            clock.fetch_add(1_500, Relaxed);
        }
        (said, store.stats("one").unwrap().keys)
    };
    let (local, local_keys) = run("local");
    let (eventual, eventual_keys) = run("eventual");
    for (i, (local, eventual)) in local.iter().zip(&eventual).enumerate() {
        assert_eq!(local, eventual, "statement {i}");
    }
    // The one difference a rule cannot see: the keys that were deleted are still
    // counted, as tombstones (`k` and `hot`).
    assert_eq!(eventual_keys, local_keys + 2);
}

/// D1: a namespace keeps its mode while rules hold it.
#[test]
fn a_namespace_cannot_change_its_mode_under_the_rules_that_hold_it() {
    let n = node("a");
    let local = NamespaceSettings::defaults(64 << 20);
    let refused = n.store.open(NS, &local).unwrap_err();
    assert_eq!(
        refused,
        "[state.shared]: consistency = \"local\", and the rules running now hold this \
         namespace as \"eventual\": a namespace cannot change its mode; declare a new one"
    );
    let _held = n.store.open("plain", &local).unwrap();
    assert!(n.store.open("plain", &eventual()).unwrap_err().contains(
        "consistency = \"eventual\", and the rules running now hold this namespace as \"local\""
    ));
    // With the rules that held it gone, the name is free.
    drop(n.ns);
    assert!(n.store.open(NS, &local).is_ok());
}

/// The T2 note T3 had to settle: a delete of a key this node holds nothing of leaves
/// nothing, not a tombstone. `kv_held` on a false condition is such a delete on
/// every message of every device that is not hot.
#[test]
fn deleting_a_key_this_node_has_never_seen_leaves_no_tombstone() {
    let mut net = Net::new(vec![node("a"), node("b")]);
    net.connect_all();
    net.settle();
    let before = net.sent;
    for i in 0..100 {
        assert_eq!(net.nodes[0].del(&format!("ghost:{i}")), "false");
        assert_eq!(
            net.nodes[0]
                .call(&StateOp::Held {
                    key: format!("cold:{i}").as_bytes(),
                    holds: false,
                    ttl: None,
                })
                .unwrap(),
            "undefined"
        );
    }
    assert_eq!(net.nodes[0].keys(), 0);
    net.settle();
    assert_eq!(net.sent, before, "nothing changed, so nothing was sent");
    // A key that was deleted is not deleted again.
    net.nodes[0].set("k", "v");
    assert_eq!(net.nodes[0].del("k"), "true");
    net.settle();
    let once = net.sent;
    assert_eq!(net.nodes[0].del("k"), "false");
    net.settle();
    assert_eq!(net.sent, once);
    assert_eq!(net.nodes[1].get("k"), "undefined");
    assert_eq!((net.nodes[0].keys(), net.nodes[1].keys()), (1, 1));
}

/// A write that leaves a key as it was is not sent: a value that is not the largest,
/// an element the set holds, a value the sketch has seen, a second `kv_set_once`.
#[test]
fn a_write_that_changes_nothing_is_not_sent() {
    let mut net = Net::new(vec![node("a"), node("b")]);
    net.connect_all();
    let a = &net.nodes[0];
    a.max("hi", 10);
    a.add("s", "x").unwrap();
    a.hll_add("h", "x");
    a.set_once("o", "first");
    net.settle();
    let before = net.sent;
    let a = &net.nodes[0];
    assert_eq!(a.max("hi", 3), "10");
    assert_eq!(a.add("s", "x").unwrap(), "1");
    assert_eq!(a.hll_add("h", "x"), "1");
    assert_eq!(a.set_once("o", "second"), "\"first\"");
    net.settle();
    assert_eq!(net.sent, before);
    net.assert_converged();
}

/// D9, for a rule's own writes: a full `eventual` namespace refuses a key that is new
/// and still updates the keys it has, as a `local` one does; a deleted key counts
/// until its window has passed, and then makes room.
#[test]
fn a_full_eventual_namespace_refuses_a_rules_new_key() {
    let small = NamespaceSettings {
        max_keys: 3,
        ..eventual()
    };
    let n = node_with("a", &small, 64 << 20);
    for key in ["k1", "k2", "k3"] {
        n.set(key, "v");
    }
    let new_key = StateOp::Incr {
        key: b"k4",
        by: 1,
        ttl: None,
    };
    assert_eq!(n.call(&new_key), Err("namespace_full".to_string()));
    n.set("k1", "still writable");
    // Deleting a key does not make room at once: the delete is kept.
    assert_eq!(n.del("k2"), "true");
    assert_eq!(n.call(&new_key), Err("namespace_full".to_string()));
    assert_eq!(n.store.stats(NS).unwrap().refused_namespace_full, 2);
    // Past the tombstone window it does, for the write that needs it.
    n.advance_ms(3_601_000);
    assert_eq!(n.call(&new_key), Ok("1".to_string()));
    assert_eq!(n.keys(), 3);

    // In bytes: a value that does not fit is refused whole, and nothing is kept of it.
    let tight = NamespaceSettings {
        max_bytes: 1_000,
        ..eventual()
    };
    let n = node_with("a", &tight, 64 << 20);
    n.set("k1", "small");
    let large = Value::from("x".repeat(900));
    let refused = n.call(&StateOp::Set {
        key: b"k2",
        value: stored(&large),
        ttl: None,
    });
    assert_eq!(refused, Err("namespace_full".to_string()));
    assert_eq!((n.keys(), n.get("k2")), (1, "undefined".into()));
    assert!(n.bytes() <= 1_000);
    // A brownout refuses a new key and nothing else.
    n.store.set_brownout(true);
    assert_eq!(n.call(&new_key), Err("brownout".to_string()));
    n.set("k1", "updated");
    assert_eq!(n.del("k1"), "true");
}

// -- convergence -----------------------------------------------------------------------

/// T3's first acceptance: two nodes updating the same keys across a partition
/// converge when it heals, each type by its own merge (D4).
#[test]
fn two_nodes_updating_the_same_keys_across_a_partition_converge_when_it_heals() {
    let mut net = Net::new(vec![node("a"), node("b")]);
    net.connect_all();
    net.nodes[0].incr("n", 1);
    net.nodes[0].set("last", "before");
    net.settle();
    net.assert_converged();
    assert_eq!(net.nodes[1].get("n"), "1");

    net.disconnect(0, 1);
    let (a, b) = (&net.nodes[0], &net.nodes[1]);
    a.incr("n", 5);
    b.incr("n", 7);
    a.add("seen", "a1").unwrap();
    b.add("seen", "b1").unwrap();
    a.max("hi", 30);
    b.max("hi", 31);
    a.hll_add("distinct", "x");
    b.hll_add("distinct", "y");
    a.set_once("first", "a was first");
    a.set("last", "from a");
    // The later write, by the clock.
    b.advance_ms(5);
    b.set_once("first", "b was second");
    b.set("last", "from b");
    assert_eq!((a.get("n"), b.get("n")), ("6".into(), "8".into()));

    net.connect_all();
    net.settle();
    net.assert_converged();
    for node in &net.nodes {
        assert_eq!(node.get("n"), "13", "{}", node.id);
        assert_eq!(node.get("seen"), r#"["a1","b1"]"#);
        assert_eq!(node.get("hi"), "31");
        assert_eq!(node.get("distinct"), "2");
        assert_eq!(node.get("first"), "\"a was first\"");
        assert_eq!(node.get("last"), "\"from b\"");
    }
    // And they go on from there as one.
    assert_eq!(net.nodes[0].incr("n", 1), "14");
    net.settle();
    assert_eq!(net.nodes[1].get("n"), "14");
    // The next comparison finds nothing to send.
    let before = net.sent;
    net.repair();
    assert_eq!(net.sent.keys, before.keys);
    assert_eq!(net.sent.digests, before.digests);
}

/// D4 as T2 amended it, between nodes: a delete covers an `lww` write stamped before
/// it and not one stamped after; for a counter it also covers the increments of a
/// node that had not heard of it; and a copy from before a delete cannot undo it.
#[test]
fn a_delete_reaches_every_node_and_an_old_copy_cannot_undo_it() {
    let mut net = Net::new(vec![node("a"), node("b"), node("c")]);
    net.connect_all();
    for key in ["k", "kept", "count"] {
        net.nodes[0].set(key, "v1");
    }
    net.nodes[0].del("count");
    net.nodes[0].incr("count", 1);
    net.settle();
    net.assert_converged();

    // C is cut off with what it has. B deletes; A, which has not heard, writes on.
    net.disconnect(0, 2);
    net.disconnect(1, 2);
    net.disconnect(0, 1);
    assert_eq!(net.nodes[1].del("k"), "true");
    assert_eq!(net.nodes[1].del("kept"), "true");
    assert_eq!(net.nodes[1].del("count"), "true");
    net.nodes[0].advance_ms(10);
    net.nodes[0].set("kept", "v2");
    net.nodes[0].incr("count", 1);

    net.connect_all();
    net.settle();
    net.assert_converged();
    for node in &net.nodes {
        assert_eq!(node.get("k"), "undefined", "{}", node.id);
        assert_eq!(node.get("kept"), "\"v2\"", "{}", node.id);
        assert_eq!(node.get("count"), "undefined", "{}", node.id);
    }
    // A counter made after hearing of the delete starts from nothing, everywhere.
    assert_eq!(net.nodes[2].incr("count", 1), "1");
    net.settle();
    assert_eq!(net.nodes[0].get("count"), "1");

    // The tombstones go when their window has passed (an hour), on every node.
    assert_eq!(net.nodes[0].keys(), 3);
    net.advance_ms(3_601_000);
    net.repair();
    for node in &net.nodes {
        assert_eq!(node.keys(), 2, "{}", node.id);
    }
    net.assert_converged();
}

/// D6: an expiry is a time on the clock, the same on every node: the key reads as
/// absent everywhere from that moment, and is forgotten without anything being sent.
#[test]
fn a_key_expires_on_every_node_at_once_and_nothing_is_sent_for_it() {
    let mut net = Net::new(vec![node("a"), node("b")]);
    net.connect_all();
    net.nodes[0].set_for("short", "v", 60);
    net.nodes[0].set("stays", "v");
    net.settle();
    assert_eq!(net.nodes[1].get("short"), "\"v\"");
    let ttl = net.nodes[1].call(&StateOp::Ttl { key: b"short" }).unwrap();
    assert_eq!(ttl, "60");

    net.advance_ms(60_000);
    for node in &net.nodes {
        assert_eq!(node.get("short"), "undefined");
    }
    let before = net.sent;
    net.repair();
    assert_eq!(net.sent.keys, before.keys);
    assert_eq!(net.sent.digests, before.digests);
    for node in &net.nodes {
        assert_eq!(node.keys(), 1);
    }
    // A write that finds it expired starts it afresh, on every node.
    net.nodes[1].incr("n", 1);
    net.nodes[1].set("short", "again");
    net.settle();
    assert_eq!(net.nodes[0].get("short"), "\"again\"");
    net.assert_converged();
}

/// Reordered, repeated and lost frames between three nodes that keep writing: once
/// the links are whole again and have compared, every node holds the same bytes.
#[test]
fn copies_converge_whatever_is_reordered_repeated_or_lost() {
    for seed in 1..=20u64 {
        let mut rng = Rng(seed);
        let mut net = Net::new(vec![node("a"), node("b"), node("c")]);
        net.connect_all();
        let mut in_flight: Vec<(usize, usize, PeerMessage)> = Vec::new();
        for round in 0..400 {
            let at = usize::try_from(rng.next(3)).unwrap();
            let node = &net.nodes[at];
            let key = format!("k{}", rng.next(12));
            // A key keeps the type its name gives it.
            match key.as_bytes()[1] % 5 {
                0 => {
                    node.set(&key, &format!("v{round}"));
                }
                1 => {
                    node.incr(&key, i64::try_from(rng.next(5)).unwrap() - 1);
                }
                2 => {
                    let _ = node.add(&key, &format!("e{}", rng.next(20)));
                }
                3 => {
                    node.max(&key, i64::try_from(rng.next(1000)).unwrap());
                }
                _ => {
                    node.hll_add(&key, &format!("e{}", rng.next(50)));
                }
            }
            if rng.next(10) == 0 {
                node.del(&key);
            }
            // Clocks that do not agree, and do not go at the same pace.
            node.advance_ms(rng.next(3));
            net.now += Duration::from_millis(rng.next(8));
            // Some frames are sent; those in flight arrive in any order, some of
            // them twice and some never.
            for (from, to) in [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)] {
                if rng.next(2) == 0 {
                    if let Some(frame) = net.take(from, to) {
                        in_flight.push((from, to, frame));
                    }
                }
            }
            while !in_flight.is_empty() && rng.next(3) != 0 {
                let at = usize::try_from(rng.next(in_flight.len() as u64)).unwrap();
                let (from, to, frame) = in_flight.swap_remove(at);
                match rng.next(10) {
                    0 => {}
                    1 => {
                        net.deliver(from, to, frame.clone());
                        in_flight.push((from, to, frame));
                    }
                    _ => net.deliver(from, to, frame),
                }
            }
        }
        // What was lost is found by comparing: a few rounds, since one repair of a
        // namespace runs at a time on a node.
        in_flight.clear();
        for _ in 0..6 {
            net.repair();
        }
        let first = net.nodes[0].snapshot();
        for node in &net.nodes[1..] {
            assert!(
                node.snapshot() == first,
                "seed {seed}: node {} differs",
                node.id
            );
        }
        let before = net.sent;
        net.repair();
        assert_eq!(net.sent.keys, before.keys, "seed {seed}: still repairing");
    }
}

// -- restart, join, leave -----------------------------------------------------------------

/// T3: a restarted node comes back empty (the store is in memory) and is refilled
/// from the others: each key once, though every member has it to offer.
#[test]
fn a_restarted_node_is_refilled_from_the_others_each_key_once() {
    const KEYS: usize = 3_000;
    let mut net = Net::new(vec![node("a"), node("b"), node("c")]);
    net.connect_all();
    for i in 0..KEYS {
        net.nodes[i % 3].set(&format!("device:{i}"), "on");
        net.nodes[i % 3].incr(&format!("count:{}", i % 50), 1);
    }
    net.settle();
    assert_eq!(net.nodes[0].del("device:7"), "true");
    net.settle();
    net.assert_converged();
    let held = net.nodes[0].keys();
    assert_eq!(held, KEYS + 50);

    // B restarts: the same name, nothing in it.
    net.disconnect(0, 1);
    net.disconnect(1, 2);
    net.nodes[1] = node("b");
    assert_eq!(net.nodes[1].keys(), 0);
    // It counts before it has heard from anyone: that is not taken, later, for
    // something it had counted before it went.
    assert_eq!(net.nodes[1].incr("count:4", 1), "1");
    let before = net.sent;
    net.connect_all();
    net.settle();
    assert_eq!(net.nodes[0].get("count:4"), "61");
    assert_eq!(net.nodes[1].get("count:4"), "61");
    assert_eq!(net.nodes[1].keys(), held);
    assert_eq!(net.nodes[1].get("device:8"), "\"on\"");
    assert_eq!(net.nodes[1].get("device:7"), "undefined");
    assert_eq!(net.nodes[1].get("count:3"), "60");
    net.assert_converged();
    // And the one key B had written, with the few that share its part, when the
    // members that did not fill it compare in their turn.
    let crossed = net.sent.keys - before.keys;
    assert!(
        (held..=held + 20).contains(&crossed),
        "{crossed} keys crossed for {held}: the namespace crosses once, not once per member"
    );
    // The members that were not asked compare at their next turn and send nothing.
    let after = net.sent;
    net.repair();
    assert_eq!(net.sent.keys, after.keys);
    // What it counts from now on is its own again, added to what it had counted.
    assert_eq!(net.nodes[1].incr("count:3", 1), "61");
}

/// "Without shipping the whole namespace when little differs": two copies of 20,000
/// keys that differ in three exchange the digest and the keys of three parts.
#[test]
fn copies_that_differ_in_a_few_keys_exchange_a_few_parts() {
    const KEYS: usize = 20_000;
    let mut net = Net::new(vec![node("a"), node("b")]);
    net.connect_all();
    for i in 0..KEYS {
        net.nodes[i % 2].set(&format!("device:{i}"), "on");
    }
    net.settle();
    net.assert_converged();

    // Three writes the other side never got: made while the link was down.
    net.disconnect(0, 1);
    net.nodes[0].set("device:10", "off");
    net.nodes[1].set("device:11", "off");
    net.nodes[1].set("new", "key");
    let before = net.sent;
    net.connect_all();
    net.settle();
    net.assert_converged();
    assert_eq!(net.nodes[1].get("device:10"), "\"off\"");
    assert_eq!(net.nodes[0].get("new"), "\"key\"");
    let (keys, digests) = (
        net.sent.keys - before.keys,
        net.sent.digests - before.digests,
    );
    assert_eq!(digests, 1, "one digest with its parts");
    // Three parts of 1,024, from both sides: about 20 keys a part.
    assert!(
        (3..=3 * 2 * 60).contains(&keys),
        "{keys} keys crossed for three that differed"
    );
}

/// A node that joins later is filled; one that leaves takes its link's place in
/// the order with it, and nothing is kept for it.
#[test]
fn a_node_joins_and_a_node_leaves() {
    let mut net = Net::new(vec![node("a"), node("b"), node("c")]);
    net.connect(0, 1);
    for i in 0..500 {
        net.nodes[i % 2].incr(&format!("k{i}"), 1);
    }
    net.settle();
    assert_eq!(net.nodes[2].keys(), 0);
    assert_eq!(net.nodes[0].replication().links, 1);

    net.connect(0, 2);
    net.connect(1, 2);
    net.nodes[2].incr("k1", 10);
    net.settle();
    net.repair();
    net.assert_converged();
    assert_eq!(net.nodes[0].get("k1"), "11");
    assert_eq!(net.nodes[0].replication().links, 2);

    // B leaves. What A writes while it is gone waits for nobody.
    net.disconnect(0, 1);
    net.disconnect(1, 2);
    for i in 0..500 {
        net.nodes[0].incr(&format!("k{i}"), 1);
    }
    net.settle();
    let a = net.nodes[0].replication();
    assert_eq!((a.links, a.pending, a.lag_ms), (1, 0, 0));
    assert_eq!(net.nodes[0].snapshot(), net.nodes[2].snapshot());
}

// -- a slow peer ---------------------------------------------------------------------------

/// T3: a key written many times while a link is slow is sent once, and what waits
/// is bounded by the number of keys, not of writes. Pending and lag say so.
#[test]
fn a_key_written_many_times_while_a_link_is_slow_is_sent_once() {
    let mut net = Net::new(vec![node("a"), node("b"), node("c")]);
    net.connect_all();
    net.settle();
    let bytes_idle = net.nodes[0].store.bytes();
    let before = net.sent;

    // The link to B takes nothing for a while; the link to C keeps up.
    for round in 0..1_000 {
        for key in 0..10 {
            net.nodes[0].incr(&format!("hot:{key}"), 1);
        }
        if round % 100 == 0 {
            while let Some(frame) = net.take(0, 2) {
                net.deliver(0, 2, frame);
            }
            net.now += Duration::from_millis(10);
        }
        net.nodes[0].advance_ms(3);
    }
    let waiting = net.nodes[0].replication();
    assert_eq!(
        waiting.pending, 10,
        "ten keys wait, not ten thousand writes"
    );
    assert!(
        (2_900..=3_000).contains(&waiting.lag_ms),
        "the oldest unsent change is as old as the slow link is behind: {} ms",
        waiting.lag_ms
    );
    // Nothing was kept for the slow link but the keys themselves.
    let per_key = (net.nodes[0].store.bytes() - bytes_idle) / 10;
    assert!(per_key < 400, "{per_key} bytes a key");

    while let Some(frame) = net.take(0, 2) {
        net.deliver(0, 2, frame);
    }
    let to_c = net.sent.keys - before.keys;
    net.settle();
    assert_eq!(
        net.sent.keys - before.keys - to_c,
        10,
        "each key crossed the slow link once"
    );
    assert_eq!(net.nodes[1].get("hot:3"), "1000");
    let caught_up = net.nodes[0].replication();
    assert_eq!((caught_up.pending, caught_up.lag_ms), (0, 0));
    net.assert_converged();
}

/// A link is given frames of bounded size, however much there is to send.
#[test]
fn a_frame_is_filled_to_its_budget_and_no_further() {
    let mut net = Net::new(vec![node("a"), node("b")]);
    net.connect_all();
    net.settle();
    let large = "x".repeat(4_000);
    for i in 0..200 {
        net.nodes[0].set(&format!("k{i}"), &large);
    }
    let mut frames = 0;
    net.now += Duration::from_millis(10);
    while let Some(frame) = net.take(0, 1) {
        let PeerMessage::StateKeys { entries, .. } = &frame else {
            panic!("{frame:?}");
        };
        let bytes: usize = entries.iter().map(|(k, s)| k.len() + s.len()).sum();
        assert!(bytes < 64 * 1024 + 4_200, "a frame of {bytes} bytes");
        frames += 1;
        net.deliver(0, 1, frame);
    }
    assert!((12..=14).contains(&frames), "{frames} frames for 800 KB");
    net.assert_converged();
}

// -- what is refused -------------------------------------------------------------------------

/// D15: rules files are per node. A frame for a namespace this node declares
/// otherwise, or not at all, is refused whole and counted, and nothing is held.
#[test]
fn a_frame_for_a_namespace_declared_otherwise_is_refused_and_counted() {
    let short_window = NamespaceSettings {
        tombstone_window_secs: 600,
        ..eventual()
    };
    let local = NamespaceSettings::defaults(64 << 20);
    let mut net = Net::new(vec![
        node("a"),
        node_with("b", &short_window, 64 << 20),
        node_with("c", &local, 64 << 20),
    ]);
    net.connect_all();
    net.settle();
    for i in 0..20 {
        net.nodes[0].set(&format!("k{i}"), "v");
    }
    net.nodes[1].set("theirs", "v");
    net.nodes[2].set("local", "v");
    net.settle();
    // A and B declare it differently: neither takes the other's.
    assert_eq!(net.nodes[1].keys(), 1);
    assert_eq!(net.nodes[0].keys(), 20);
    assert!(net.nodes[1].replication().refused(Refused::Declaration) >= 2);
    assert!(net.nodes[0].replication().refused(Refused::Declaration) >= 2);
    // C declares it `local`: it holds its own key, and counts what it was sent.
    assert_eq!(net.nodes[2].keys(), 1);
    assert_eq!(net.nodes[2].get("k1"), "undefined");
    assert!(net.nodes[2].store.replication_undeclared() >= 2);
    assert!(net.nodes[2].store.replication(NS).is_none());
    assert_eq!(net.nodes[0].store.replication_undeclared(), 0);
    // A bound is not part of what must agree: a node may hold fewer keys.
    let fewer = NamespaceSettings {
        max_keys: 5,
        ..eventual()
    };
    let mut net = Net::new(vec![node("a"), node_with("b", &fewer, 64 << 20)]);
    net.connect_all();
    net.nodes[0].set("k", "v");
    net.settle();
    assert_eq!(net.nodes[1].get("k"), "\"v\"");
}

/// D15, D9: a node at a bound takes a replicated key only while it has room. What
/// does not fit is refused and counted, what it holds stays, and no bound is passed.
#[test]
fn a_merge_keeps_to_the_namespaces_bounds_and_the_stores_ceiling() {
    // `max_keys`.
    let five = NamespaceSettings {
        max_keys: 5,
        ..eventual()
    };
    let mut net = Net::new(vec![node("a"), node_with("b", &five, 64 << 20)]);
    net.connect_all();
    for i in 0..20 {
        net.nodes[0].set(&format!("k{i}"), "v");
    }
    net.settle();
    assert_eq!(net.nodes[1].keys(), 5);
    assert_eq!(
        net.nodes[1].replication().refused(Refused::NamespaceFull),
        15
    );
    // The repair offers them again, and they are refused again.
    net.repair();
    assert_eq!(net.nodes[1].keys(), 5);
    assert!(net.nodes[1].replication().refused(Refused::NamespaceFull) >= 30);
    // A key it holds still merges.
    let held: Vec<String> = (0..20)
        .map(|i| format!("k{i}"))
        .filter(|k| net.nodes[1].get(k) == "\"v\"")
        .collect();
    net.nodes[0].set(&held[0], "next");
    net.settle();
    assert_eq!(net.nodes[1].get(&held[0]), "\"next\"");

    // `max_bytes`: new keys, and a key that would grow.
    let small = NamespaceSettings {
        max_bytes: 2_000,
        ..eventual()
    };
    let mut net = Net::new(vec![node("a"), node_with("b", &small, 64 << 20)]);
    net.connect_all();
    net.nodes[0].add("set", "first").unwrap();
    net.settle();
    assert_eq!(net.nodes[1].get("set"), r#"["first"]"#);
    for i in 0..200 {
        net.nodes[0].add("set", &format!("element-{i}")).unwrap();
    }
    for i in 0..20 {
        net.nodes[0].set(&format!("k{i}"), &"x".repeat(100));
    }
    net.settle();
    let b = &net.nodes[1];
    assert!(b.bytes() <= 2_000, "{} bytes", b.bytes());
    assert_eq!(b.get("set"), r#"["first"]"#, "the set it held is as it was");
    assert!(b.replication().refused(Refused::NamespaceFull) >= 15);

    // The store's ceiling, with another namespace holding most of it.
    let roomy = eventual();
    let b = node_with("b", &roomy, 3_000);
    let other = b
        .store
        .open("other", &NamespaceSettings::defaults(3_000))
        .unwrap();
    let filler = Value::from("y".repeat(900));
    for key in [&b"1"[..], b"2"] {
        other
            .call(&StateOp::Set {
                key,
                value: stored(&filler),
                ttl: None,
            })
            .unwrap();
    }
    let mut net = Net::new(vec![node("a"), b]);
    net.connect_all();
    for i in 0..20 {
        net.nodes[0].set(&format!("k{i}"), "v");
    }
    net.settle();
    let b = &net.nodes[1];
    assert!(b.store.bytes() <= 3_000, "{} bytes", b.store.bytes());
    assert!(b.keys() < 20);
    assert!(b.replication().refused(Refused::StoreFull) >= 1);

    // A brownout: no new key, and the keys it holds still merge.
    let mut net = Net::new(vec![node("a"), node("b")]);
    net.connect_all();
    net.nodes[0].set("held", "v1");
    net.settle();
    net.nodes[1].store.set_brownout(true);
    net.nodes[0].set("held", "v2");
    net.nodes[0].set("new", "v");
    net.settle();
    let b = &net.nodes[1];
    assert_eq!(
        (b.get("held"), b.get("new")),
        ("\"v2\"".into(), "undefined".into())
    );
    assert_eq!(b.replication().refused(Refused::Brownout), 1);
    b.store.set_brownout(false);
    net.repair();
    assert_eq!(net.nodes[1].get("new"), "\"v\"");
}

/// D6, as T2 bounded it: a state stamped more than 60 seconds ahead of this node's
/// clock is refused and counted, and taken once the clock has caught up.
#[test]
fn a_state_from_a_clock_too_far_ahead_is_refused_until_the_clock_catches_up() {
    let mut net = Net::new(vec![node("a"), node("b")]);
    net.connect_all();
    net.nodes[0].advance_ms(120_000);
    net.nodes[0].set("k", "from the future");
    net.nodes[0].set_once("first", "from the future");
    // A counter carries no stamp: there is no clock in it to follow.
    net.nodes[0].incr("n", 1);
    net.settle();
    let b = &net.nodes[1];
    assert_eq!(b.get("k"), "undefined");
    assert_eq!(b.get("first"), "undefined");
    assert_eq!(b.replication().refused(Refused::ClockSkew), 2);
    assert_eq!((b.keys(), b.get("n")), (1, "1".into()));
    // B's own clock was not moved: what it writes is stamped by its own time, so
    // A's write, two minutes later, still wins.
    b.set("k", "from b");
    net.settle();
    assert_eq!(net.nodes[0].get("k"), "\"from the future\"");

    // B's clock comes within the bound: the next repair brings the states over.
    net.nodes[1].advance_ms(61_000);
    net.repair();
    assert_eq!(net.nodes[1].get("k"), "\"from the future\"");
    assert_eq!(net.nodes[1].get("first"), "\"from the future\"");
    net.assert_converged();
}

/// A buggy or hostile peer: frames that are not what they should be are refused
/// piece by piece and counted, the good keys among them are still taken, and
/// whatever arrives the namespace stays within its bounds.
#[test]
fn malformed_frames_are_refused_and_never_pass_a_bound() {
    let bounded = NamespaceSettings {
        max_keys: 50,
        max_bytes: 20_000,
        ..eventual()
    };
    let a = node("a");
    a.set("good", "v");
    a.add("set", "e").unwrap();
    let good = a.snapshot();
    let b = node_with("b", &bounded, 64 << 20);
    let declaration = super::replica::declaration_of(&bounded);
    let mut link = StateLink::new(b.store.clone(), "b", "a");
    let now = Instant::now();
    let keys = |entries: Vec<(Vec<u8>, Vec<u8>)>| PeerMessage::StateKeys {
        namespace: NS.to_string(),
        declaration,
        entries,
    };
    let state = good[0].1.clone();
    link.receive_at(
        keys(vec![
            (Vec::new(), state.clone()),
            (vec![b'k'; 257], state.clone()),
            (b"cut".to_vec(), state[..state.len() - 1].to_vec()),
            (b"junk".to_vec(), vec![0xff; 40]),
            (b"empty".to_vec(), Vec::new()),
            good[0].clone(),
        ]),
        now,
    );
    assert_eq!(b.replication().refused(Refused::Malformed), 5);
    assert_eq!(b.get("good"), "\"v\"");
    assert_eq!(b.keys(), 1);

    // Summaries of the wrong size.
    for parts in [1, STATE_PARTS - 1, STATE_PARTS + 1] {
        link.receive_at(
            PeerMessage::StateDigest {
                namespace: NS.to_string(),
                declaration,
                root: 1,
                parts: vec![0; parts],
            },
            now,
        );
    }
    for parts in [0, 1, STATE_PARTS / 64 + 1, STATE_PARTS] {
        link.receive_at(
            PeerMessage::StateWant {
                namespace: NS.to_string(),
                declaration,
                parts: vec![u64::MAX; parts],
            },
            now,
        );
    }
    assert_eq!(b.replication().refused(Refused::Malformed), 12);

    // Anything at all, and real states with a byte changed: nothing panics, and the
    // bounds hold after every frame.
    let mut rng = Rng(7);
    for _ in 0..3_000 {
        let mut entries = Vec::new();
        for _ in 0..rng.next(4) {
            let key: Vec<u8> = (0..rng.next(40)).map(|_| rng.byte()).collect();
            let mut state = good[usize::try_from(rng.next(2)).unwrap()].1.clone();
            match rng.next(3) {
                0 => state = (0..rng.next(80)).map(|_| rng.byte()).collect(),
                1 => {
                    let at = usize::try_from(rng.next(state.len() as u64)).unwrap();
                    state[at] = rng.byte();
                }
                _ => {}
            }
            entries.push((key, state));
        }
        link.receive_at(keys(entries), now);
        assert!(b.keys() <= 50, "{} keys", b.keys());
        assert!(b.bytes() <= 20_000, "{} bytes", b.bytes());
    }
    assert!(b.replication().refused(Refused::NamespaceFull) > 0);
    assert!(b.replication().refused(Refused::Malformed) > 12);
}

/// D15: "a cluster that uses no such namespace puts nothing new on the wire".
#[test]
fn a_store_without_an_eventual_namespace_sends_nothing() {
    let store = Store::with_manual_clock(64 << 20, Arc::new(AtomicU64::new(START_MS)));
    store.set_node("a");
    let local = store
        .open("plain", &NamespaceSettings::defaults(64 << 20))
        .unwrap();
    let value = Value::from("v");
    local
        .call(&StateOp::Set {
            key: b"k",
            value: stored(&value),
            ttl: None,
        })
        .unwrap();
    let mut link = StateLink::new(store.clone(), "a", "b");
    let mut now = Instant::now();
    for _ in 0..100 {
        assert!(link.poll(now).is_none());
        now += Duration::from_mins(1);
    }
    // Declared later (a reload): the link takes it up without being told to.
    let shared = store.open(NS, &eventual()).unwrap();
    assert!(matches!(
        link.poll(now),
        Some(PeerMessage::StateDigest { parts, .. }) if parts.is_empty()
    ));
    shared
        .call(&StateOp::Set {
            key: b"k",
            value: stored(&value),
            ttl: None,
        })
        .unwrap();
    now += Duration::from_millis(10);
    assert!(matches!(
        link.poll(now),
        Some(PeerMessage::StateKeys { entries, .. }) if entries.len() == 1
    ));
    // And dropped again: nothing more.
    drop(shared);
    now += Duration::from_mins(1);
    assert!(link.poll(now).is_none());
}

/// Many connection tasks write while a link reads the order of change and another
/// node's states are merged in: every change gets across, and the counts are exact.
#[test]
fn concurrent_writers_a_reader_and_a_merger_lose_nothing() {
    const THREADS: usize = 6;
    const EACH: usize = 4_000;
    let mut net = Net::new(vec![node("a"), node("b")]);
    net.connect_all();
    net.settle();
    let finished = std::sync::atomic::AtomicUsize::new(0);
    {
        let Net {
            nodes, links, now, ..
        } = &mut net;
        std::thread::scope(|scope| {
            for t in 0..THREADS {
                let (a, b) = (nodes[0].ns.clone(), nodes[1].ns.clone());
                let finished = &finished;
                scope.spawn(move || {
                    for i in 0..EACH {
                        for ns in [&a, &b] {
                            ns.call(&StateOp::Incr {
                                key: b"hot",
                                by: 1,
                                ttl: None,
                            })
                            .unwrap();
                        }
                        let key = format!("{t}:{}", i % 200);
                        a.call(&StateOp::Incr {
                            key: key.as_bytes(),
                            by: 1,
                            ttl: None,
                        })
                        .unwrap();
                    }
                    finished.fetch_add(1, Relaxed);
                });
            }
            // The links run for as long as the writers do.
            let finished = &finished;
            scope.spawn(move || {
                while finished.load(Relaxed) < THREADS {
                    for (from, to) in [(0, 1), (1, 0)] {
                        let frame = links.get_mut(&(from, to)).and_then(|link| link.poll(*now));
                        if let (Some(frame), Some(link)) = (frame, links.get_mut(&(to, from))) {
                            link.receive_at(frame, *now);
                        }
                    }
                    *now += Duration::from_millis(10);
                }
            });
        });
    }
    net.settle();
    net.assert_converged();
    let total = 2 * THREADS * EACH;
    assert_eq!(net.nodes[0].get("hot"), total.to_string());
    assert_eq!(net.nodes[1].get("0:7"), (EACH / 200).to_string());
    assert_eq!(net.nodes[1].keys(), 1 + THREADS * 200);
}
