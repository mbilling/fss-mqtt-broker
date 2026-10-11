//! `$events/state/expired` for the keys of an `eventual` namespace (ADR 0085 D7, the
//! cluster half of T4): which member raises a key's event, what the others keep
//! until it has, and what happens when it goes first.
//!
//! As in the file above, nothing waits for time: each node's clock is a number, the
//! sweep is called by hand with the number of events it may hand over, and a frame
//! crosses a link when the test says so.

use std::collections::BTreeMap;

use mqtt_cluster::NodeId;

use super::*;
use crate::rule_state::expiry::Pace;

/// Nodes whose rules all select the expiry event, every link up, and each told of
/// the others.
fn raising(ids: &[&str]) -> Net {
    let mut net = Net::new(ids.iter().map(|id| node(id)).collect());
    for node in &net.nodes {
        node.store.set_expiry_events(true);
    }
    net.connect_all();
    net.settle();
    net
}

/// The member the cluster's rendezvous hash picks for `key` among `ids`.
fn picked(ids: &[&str], key: &str) -> String {
    let nodes: Vec<NodeId> = ids.iter().map(|id| NodeId((*id).to_string())).collect();
    let named = [NS.as_bytes(), &[0], key.as_bytes()].concat();
    mqtt_cluster::hrw::owner(&named, &nodes).unwrap().0.clone()
}

/// A key named `<prefix><n>` that `owner` is picked for among `ids`.
fn key_of(ids: &[&str], owner: &str, prefix: &str) -> String {
    (0..10_000)
        .map(|n| format!("{prefix}{n}"))
        .find(|key| picked(ids, key) == owner)
        .unwrap()
}

impl Node {
    /// One pass of this node's sweep, handing over at most `max` events: the keys
    /// they were raised for, with the value each held.
    fn sweep(&self, max: usize) -> Vec<(String, String)> {
        self.store
            .sweep(max)
            .expired
            .into_iter()
            .map(|expired| {
                assert_eq!(&*expired.namespace, NS);
                (
                    String::from_utf8_lossy(&expired.key).into_owned(),
                    expired.value.to_json().unwrap(),
                )
            })
            .collect()
    }

    /// The keys whose expiry this node has dealt with, and those it has yet to.
    fn expiries(&self) -> (u64, u64) {
        let counts = self.store.counts(NS).unwrap();
        (counts.expired, counts.expiry_backlog)
    }
}

impl Net {
    /// Every node sweeps, with no bound on the events, and the links run until
    /// quiet; twice, so that what a node was told by the first pass is acted on.
    /// The events raised, by key: which nodes raised one for it.
    fn sweep_all(&mut self) -> BTreeMap<String, Vec<String>> {
        let mut raised: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for _ in 0..2 {
            for node in &self.nodes {
                for (key, _) in node.sweep(usize::MAX) {
                    raised.entry(key).or_default().push(node.id.clone());
                }
            }
            self.settle();
        }
        raised
    }

    fn ids(&self) -> Vec<&str> {
        self.nodes.iter().map(|node| node.id.as_str()).collect()
    }
}

/// D7's "one node in a stable cluster": every key's event is raised once, by the
/// member the hash of the namespace and the key picks, whichever node wrote the key;
/// the others keep the key until that member says it has got past it, and then hold
/// nothing.
#[test]
fn in_a_stable_cluster_one_member_raises_each_keys_event() {
    let mut net = raising(&["a", "b", "c"]);
    for n in 0..300 {
        net.nodes[n % 3].set_for(&format!("k{n}"), "v", 10);
    }
    net.settle();
    net.assert_converged();

    net.advance_ms(11_000);
    // Read as absent on every node from the moment it expired.
    for node in &net.nodes {
        assert_eq!(node.get("k7"), "undefined");
    }
    let raised = net.sweep_all();
    assert_eq!(raised.len(), 300, "an event for every key");
    let ids = net.ids();
    for (key, by) in &raised {
        assert_eq!(
            by,
            &[picked(&ids, key)],
            "{key}: once, by the picked member"
        );
    }
    for id in &ids {
        let share = raised.values().filter(|by| by[0] == *id).count();
        assert!(share > 50, "{id} raised {share} of 300");
    }
    // Nothing is kept anywhere, each node has dealt with each expiry once, and a
    // further pass raises nothing.
    for node in &net.nodes {
        assert_eq!(node.keys(), 0, "{}", node.id);
        assert_eq!(node.bytes(), 0, "{}", node.id);
        assert_eq!(node.expiries(), (300, 0), "{}", node.id);
    }
    net.advance_ms(5_000);
    assert!(net.sweep_all().is_empty());
}

/// While its event is owed a key reads as absent and still counts toward the
/// namespace's bounds, on the member that raises it and on those that stand by.
#[test]
fn a_key_that_waits_for_its_event_reads_as_absent_and_keeps_its_room() {
    let mut net = raising(&["a", "b"]);
    net.nodes[0].set_for("k", "v", 10);
    net.settle();
    let held = net.nodes[0].bytes();
    net.advance_ms(11_000);
    // A pass that may hand nothing over: the key is found, and kept.
    for node in &net.nodes {
        assert!(node.sweep(0).is_empty());
        assert_eq!(node.get("k"), "undefined");
        assert_eq!((node.keys(), node.bytes()), (1, held), "{}", node.id);
        assert_eq!(node.expiries(), (0, 1), "{}", node.id);
    }
    let raised = net.sweep_all();
    assert_eq!(raised["k"], [picked(&["a", "b"], "k")]);
    for node in &net.nodes {
        assert_eq!((node.keys(), node.bytes()), (0, 0), "{}", node.id);
        assert_eq!(node.expiries(), (1, 0), "{}", node.id);
    }
}

/// The picked member goes before it has raised anything: the members that are left
/// pick among themselves, and each key's event is raised by one of them.
#[test]
fn a_picked_member_that_goes_before_it_raised_leaves_its_keys_to_the_others() {
    let mut net = raising(&["a", "b", "c"]);
    for n in 0..150 {
        net.nodes[n % 3].set_for(&format!("k{n}"), "v", 10);
    }
    net.settle();
    net.advance_ms(11_000);
    // Every node finds the keys expired; none hands an event over yet.
    for node in &net.nodes {
        assert!(node.sweep(0).is_empty());
        assert_eq!(node.expiries(), (0, 150), "{}", node.id);
    }
    // `a` is gone, with what it was to raise.
    net.disconnect(0, 1);
    net.disconnect(0, 2);
    let mut raised: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for _ in 0..2 {
        for node in &net.nodes[1..] {
            for (key, _) in node.sweep(usize::MAX) {
                raised.entry(key).or_default().push(node.id.clone());
            }
        }
        net.settle();
    }
    assert_eq!(raised.len(), 150, "none lost");
    for (key, by) in &raised {
        assert_eq!(by, &[picked(&["b", "c"], key)], "{key}");
    }
    for node in &net.nodes[1..] {
        assert_eq!(node.expiries(), (150, 0), "{}", node.id);
    }
}

/// At least once: a member that raised some events and went before it could say so
/// has those raised again by the others, and no others are.
#[test]
fn events_a_member_raised_and_could_not_say_so_are_raised_again() {
    let mut net = raising(&["a", "b", "c"]);
    for n in 0..150 {
        net.nodes[n % 3].set_for(&format!("k{n}"), "v", 10);
    }
    net.settle();
    net.advance_ms(11_000);
    for node in &net.nodes[1..] {
        assert!(node.sweep(0).is_empty());
    }
    // `a` hands over twenty and stops before any link has carried the news.
    let by_a: Vec<String> = net.nodes[0].sweep(20).into_iter().map(|(k, _)| k).collect();
    assert_eq!(by_a.len(), 20);
    net.disconnect(0, 1);
    net.disconnect(0, 2);
    let mut by_others: BTreeMap<String, usize> = BTreeMap::new();
    for _ in 0..2 {
        for node in &net.nodes[1..] {
            for (key, _) in node.sweep(usize::MAX) {
                *by_others.entry(key).or_default() += 1;
            }
        }
        net.settle();
    }
    assert_eq!(by_others.len(), 150, "the others raise every key");
    assert!(by_others.values().all(|times| *times == 1));
    assert!(by_a.iter().all(|key| by_others.contains_key(key)));
}

/// What a member says of how far it has got is what lets the others go: a member
/// that has raised a second's keys says so at once, and one that is working through
/// a backlog says nothing it has not done.
#[test]
fn the_others_let_go_only_of_what_the_picked_member_has_got_past() {
    let ids = ["a", "b"];
    let mut net = raising(&ids);
    // Two seconds' worth of keys that `a` is picked for.
    let early: Vec<String> = (0..3)
        .map(|n| key_of(&ids, "a", &format!("early{n}-")))
        .collect();
    let late = key_of(&ids, "a", "late-");
    for key in &early {
        net.nodes[1].set_for(key, "v", 10);
    }
    net.nodes[1].set_for(&late, "v", 12);
    net.settle();
    net.advance_ms(13_000);
    assert!(net.nodes[1].sweep(usize::MAX).is_empty(), "b raises none");
    assert_eq!(net.nodes[1].expiries(), (0, 4));
    // `a` hands over two of the first second's three: it is not past that second.
    assert_eq!(net.nodes[0].sweep(2).len(), 2);
    net.settle();
    assert!(net.nodes[1].sweep(usize::MAX).is_empty());
    assert_eq!(net.nodes[1].expiries(), (0, 4), "b still keeps all four");
    // The third: past the first second, not the second.
    assert_eq!(net.nodes[0].sweep(1).len(), 1);
    net.settle();
    assert!(net.nodes[1].sweep(usize::MAX).is_empty());
    assert_eq!(net.nodes[1].expiries(), (3, 1));
    assert_eq!(net.nodes[0].sweep(1), [(late, "\"v\"".to_string())]);
    net.settle();
    assert!(net.nodes[1].sweep(usize::MAX).is_empty());
    assert_eq!(net.nodes[1].expiries(), (4, 0));
    assert_eq!(net.nodes[1].keys(), 0);
}

/// Two sides of a partition each pick among the members they reach, so a key's event
/// is raised on each side; when it heals nothing is raised again and the copies
/// agree.
#[test]
fn each_side_of_a_partition_raises_and_healing_raises_nothing_more() {
    let mut net = raising(&["a", "b", "c"]);
    for n in 0..90 {
        net.nodes[n % 3].set_for(&format!("k{n}"), "v", 10);
    }
    net.settle();
    net.disconnect(0, 1);
    net.disconnect(0, 2);
    net.advance_ms(11_000);
    let raised = net.sweep_all();
    assert_eq!(raised.len(), 90);
    for (key, by) in &raised {
        assert_eq!(
            by,
            &["a".to_string(), picked(&["b", "c"], key)],
            "{key}: once on each side"
        );
    }
    net.connect(0, 1);
    net.connect(0, 2);
    net.repair();
    net.advance_ms(5_000);
    assert!(net.sweep_all().is_empty());
    net.assert_converged();
    for node in &net.nodes {
        assert_eq!(node.keys(), 0, "{}", node.id);
    }
}

/// "Silent for X" is silent as the picked member sees it. A refresh made on another
/// node that reaches the picked member before the key expires there moves the expiry
/// and no event is raised; one that has not arrived by then does not undo the expiry:
/// the event is raised, with the value the key held, and the key lives on from the
/// refresh.
#[test]
fn a_refresh_on_another_node_stops_the_event_only_if_it_arrives_before_the_expiry() {
    let ids = ["a", "b"];
    let mut net = raising(&ids);
    let (in_time, late, mid) = (
        key_of(&ids, "a", "in-time-"),
        key_of(&ids, "a", "late-"),
        key_of(&ids, "a", "mid-"),
    );
    for key in [&in_time, &late, &mid] {
        net.nodes[1].set_for(key, "first", 10);
    }
    net.settle();

    // Nine seconds on, `b` refreshes all three. One refresh reaches `a` at once.
    net.advance_ms(9_000);
    net.nodes[1].set_for(&in_time, "second", 10);
    net.settle();
    net.nodes[1].set_for(&late, "second", 10);
    net.nodes[1].set_for(&mid, "second", 10);
    let delayed = net.take(1, 0).expect("the two refreshes, in one frame");

    // The old expiry passes. `mid`'s refresh arrives after it and before the sweep:
    // the key had expired here, and its event is owed with what it held.
    net.advance_ms(2_000);
    let PeerMessage::StateKeys {
        namespace,
        declaration,
        entries,
    } = delayed
    else {
        panic!("expected keys, got {delayed:?}");
    };
    let (now, later): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .partition(|(key, _)| key == mid.as_bytes());
    let frame = |entries| PeerMessage::StateKeys {
        namespace: namespace.clone(),
        declaration,
        entries,
    };
    net.deliver(1, 0, frame(now));
    assert_eq!(net.nodes[0].get(&mid), "\"second\"", "and the key lives on");
    let mut raised = net.nodes[0].sweep(usize::MAX);
    raised.sort();
    assert_eq!(
        raised,
        [
            (late.clone(), "\"first\"".to_string()),
            (mid.clone(), "\"first\"".to_string()),
        ],
        "not for the key whose refresh came in time"
    );
    // `late`'s refresh arrives after its event: the key is there again.
    net.deliver(1, 0, frame(later));
    net.settle();
    assert_eq!(net.nodes[0].get(&late), "\"second\"");
    assert!(net.sweep_all().is_empty());
    net.assert_converged();

    // Silent from here on: each of the three expires once more, and is raised once.
    net.advance_ms(9_000);
    let raised = net.sweep_all();
    assert_eq!(raised.len(), 3);
    assert!(raised.values().all(|by| by == &["a".to_string()]));
    for node in &net.nodes {
        assert_eq!(node.keys(), 0, "{}", node.id);
    }
}

/// A rule writes a key that has expired and whose event is owed: the write creates
/// the key anew, and the event is still raised with what the key held, as on one
/// node. What it held needs room of its own until then.
#[test]
fn a_write_to_an_expired_key_does_not_lose_its_event() {
    let ids = ["a", "b"];
    let mut net = raising(&ids);
    let key = key_of(&ids, "a", "k");
    net.nodes[0].set_for(&key, "old", 10);
    net.settle();
    net.advance_ms(11_000);
    // Written on the node that stands by, before either has swept.
    net.nodes[1].set_for(&key, "new", 10);
    net.settle();
    assert_eq!(net.nodes[0].get(&key), "\"new\"");
    assert_eq!(net.nodes[0].keys(), 2, "what it held, and what it holds");
    let raised = net.sweep_all();
    assert_eq!(raised[&key], ["a"]);
    for node in &net.nodes {
        assert_eq!(node.keys(), 1, "{}", node.id);
        assert_eq!(node.get(&key), "\"new\"");
    }
    net.assert_converged();

    // A namespace with no room for both refuses the write until the event is out.
    let tight = NamespaceSettings {
        max_keys: 1,
        ..eventual()
    };
    let one = node_with("z", &tight, 64 << 20);
    one.store.set_expiry_events(true);
    one.set_for("k", "old", 10);
    one.advance_ms(11_000);
    let value = Value::from("new");
    let write = StateOp::Set {
        key: b"k",
        value: stored(&value),
        ttl: Some(10),
    };
    assert_eq!(one.call(&write).unwrap_err(), "namespace_full");
    assert_eq!(one.sweep(usize::MAX), [("k".into(), "\"old\"".into())]);
    one.call(&write).unwrap();
}

/// A key deleted and written again holds its delete for the tombstone window, so it
/// cannot be forgotten when it expires: it stays in every copy as it is, and its
/// event is raised once all the same, at its expiry and not at the window's end.
#[test]
fn a_key_that_cannot_be_forgotten_yet_raises_its_event_at_its_expiry() {
    let ids = ["a", "b", "c"];
    let mut net = raising(&ids);
    for n in 0..30 {
        let key = format!("k{n}");
        net.nodes[n % 3].set(&key, "v");
        net.settle();
        assert_eq!(net.nodes[(n + 1) % 3].del(&key), "true");
        net.settle();
        net.nodes[(n + 2) % 3].set_for(&key, "again", 10);
    }
    net.settle();
    net.advance_ms(11_000);
    let raised = net.sweep_all();
    assert_eq!(raised.len(), 30);
    for (key, by) in &raised {
        assert_eq!(by, &[picked(&ids, key)], "{key}");
    }
    for node in &net.nodes {
        assert_eq!(node.keys(), 30, "{}: the deletes are remembered", node.id);
        assert_eq!(node.expiries(), (30, 0), "{}", node.id);
    }
    net.assert_converged();
    // The window ends: forgotten, and nothing is raised a second time.
    net.advance_ms(3_600_000);
    assert!(net.sweep_all().is_empty());
    for node in &net.nodes {
        assert_eq!(node.keys(), 0, "{}", node.id);
        assert_eq!(node.expiries(), (30, 0), "{}", node.id);
    }
}

/// A node that is filled by the others is sent the keys they still remember, those
/// that expired before it held them among them: no event is owed for those here, and
/// none is raised a second time.
#[test]
fn a_node_filled_with_keys_that_had_already_expired_raises_nothing_for_them() {
    let ids = ["a", "b", "c"];
    let mut net = raising(&ids);
    for n in 0..30 {
        let key = format!("k{n}");
        net.nodes[n % 3].set(&key, "v");
        net.settle();
        assert_eq!(net.nodes[(n + 1) % 3].del(&key), "true");
        net.settle();
        net.nodes[(n + 2) % 3].set_for(&key, "again", 10);
    }
    net.settle();
    net.advance_ms(11_000);
    assert_eq!(net.sweep_all().len(), 30);
    // `c` restarts and is filled: it holds the thirty keys again, as expired as the
    // others hold them.
    net.disconnect(0, 2);
    net.disconnect(1, 2);
    net.nodes[2] = node("c");
    net.nodes[2].store.set_expiry_events(true);
    net.nodes[2]
        .clock
        .store(net.nodes[0].clock.load(Relaxed), Relaxed);
    net.connect(0, 2);
    net.connect(1, 2);
    net.repair();
    net.assert_converged();
    assert_eq!(net.nodes[2].keys(), 30);
    net.advance_ms(2_000);
    assert!(net.sweep_all().is_empty());
    assert_eq!(net.nodes[2].expiries(), (0, 0));
}

/// D8's remembered idempotency keys are entries of the namespace, and expire like
/// any: without an event, on every member, never kept for one and never behind the
/// events' pace.
#[test]
fn a_remembered_idempotency_key_expires_on_every_member_without_an_event() {
    let mut net = raising(&["a", "b", "c"]);
    for n in 0..60 {
        let id = format!("message-{n}");
        let op = StateOp::IncrOnce {
            key: b"count",
            by: 1,
            ttl: Some(3_600),
            once: id.as_bytes(),
        };
        net.nodes[n % 3].call(&op).unwrap();
        net.nodes[n % 3].set_for(&format!("k{n}"), "v", 300);
    }
    net.settle();
    net.assert_converged();
    for node in &net.nodes {
        assert_eq!(node.get("count"), "60");
        assert_eq!(
            node.keys(),
            121,
            "{}: the counter, 60 keys, 60 entries",
            node.id
        );
    }
    // The dedupe window (five minutes) and the keys' own five minutes pass. A pass
    // that may hand no event over forgets every remembered key all the same.
    net.advance_ms(301_000);
    for node in &net.nodes {
        assert!(node.sweep(0).is_empty());
        assert_eq!(node.keys(), 61, "{}: the entries are gone", node.id);
        assert_eq!(node.expiries(), (60, 60), "{}", node.id);
    }
    let raised = net.sweep_all();
    assert_eq!(raised.len(), 60, "the keys' events, and no entry's");
    assert!(raised.keys().all(|key| key.starts_with('k')));
    assert!(raised.values().all(|by| by.len() == 1));
    for node in &net.nodes {
        assert_eq!(node.keys(), 1, "{}", node.id);
        assert_eq!(node.expiries(), (120, 0), "{}", node.id);
    }
    net.assert_converged();
}

/// What waits for its event is no part of what is replicated: copies with keys
/// waiting, of both kinds, on a member that raises, on members that stand by and
/// beside a member whose rules do not select the event at all, have the same
/// digests, and the links carry the periodic roots and the word on expiry, and no
/// key and no digest's parts.
#[test]
fn keys_that_wait_for_their_event_cause_no_repair_traffic() {
    let mut net = Net::new(vec![node("a"), node("b"), node("c"), node("d")]);
    for node in &net.nodes[..3] {
        node.store.set_expiry_events(true);
    }
    net.connect_all();
    net.settle();
    for n in 0..120 {
        let key = format!("k{n}");
        if n % 2 == 0 {
            // One that cannot be forgotten when it expires.
            net.nodes[n % 4].set(&key, "v");
            net.settle();
            assert_eq!(net.nodes[(n + 1) % 4].del(&key), "true");
            net.settle();
        }
        net.nodes[(n + 2) % 4].set_for(&key, "v", 10);
    }
    net.settle();
    net.advance_ms(11_000);
    // Every node finds the keys expired; no event is handed over.
    for node in &net.nodes {
        assert!(node.sweep(0).is_empty());
    }
    for node in &net.nodes[..3] {
        assert_eq!(node.expiries(), (0, 120), "{}", node.id);
        assert_eq!(node.keys(), 120, "{}", node.id);
    }
    assert_eq!(net.nodes[3].expiries(), (120, 0), "d owes no event");
    assert_eq!(
        net.nodes[3].keys(),
        60,
        "d keeps the deletes it must remember"
    );
    net.settle();

    let before = net.sent;
    let mut kinds: BTreeMap<&str, usize> = BTreeMap::new();
    for _ in 0..5 {
        net.now += Duration::from_secs(31);
        for _ in 0..1_000 {
            let links: Vec<(usize, usize)> = net.links.keys().copied().collect();
            let mut any = false;
            for (from, to) in links {
                if let Some(frame) = net.take(from, to) {
                    *kinds
                        .entry(match &frame {
                            PeerMessage::StateDigest { parts, .. } if parts.is_empty() => "root",
                            PeerMessage::StateExpiry { .. } => "expiry",
                            _ => "other",
                        })
                        .or_default() += 1;
                    net.deliver(from, to, frame);
                    any = true;
                }
            }
            if !any {
                break;
            }
        }
        // The sweep runs on, and still may hand nothing over.
        for node in &net.nodes {
            assert!(node.sweep(0).is_empty());
        }
    }
    assert_eq!(net.sent.keys, before.keys, "no key crossed a link");
    assert_eq!(net.sent.digests, before.digests, "no digest's parts did");
    // Five rounds: a root each way on six links, and the three members that raise
    // the event saying so on their three links each.
    assert_eq!(
        kinds,
        BTreeMap::from([("expiry", 5 * 3 * 3), ("root", 5 * 12)])
    );
    net.assert_converged();
    for node in &net.nodes[..3] {
        assert_eq!(node.expiries(), (0, 120), "{}", node.id);
    }
}

/// Per-node rules files: only a member whose rules select the event is picked, and
/// one whose rules do not forgets the key as it always did.
#[test]
fn only_members_whose_rules_select_the_event_are_picked() {
    let mut net = Net::new(vec![node("a"), node("b"), node("c")]);
    net.nodes[1].store.set_expiry_events(true);
    net.nodes[2].store.set_expiry_events(true);
    net.connect_all();
    net.settle();
    for n in 0..90 {
        net.nodes[n % 3].set_for(&format!("k{n}"), "v", 10);
    }
    net.settle();
    net.advance_ms(11_000);
    let raised = net.sweep_all();
    assert_eq!(raised.len(), 90);
    for (key, by) in &raised {
        assert_eq!(by, &[picked(&["b", "c"], key)], "{key}");
    }
    assert_eq!(net.nodes[0].expiries(), (90, 0));

    // `c`'s rules are reloaded without the rule: `b` is told, and raises alone.
    net.nodes[2].store.set_expiry_events(false);
    net.settle();
    for n in 0..30 {
        net.nodes[n % 3].set_for(&format!("j{n}"), "v", 10);
    }
    net.settle();
    net.advance_ms(11_000);
    let raised = net.sweep_all();
    assert_eq!(raised.len(), 30);
    assert!(raised.values().all(|by| by == &["b".to_string()]));
}

/// A reload that leaves no rule on the event lets go of what waited for it.
#[test]
fn keys_that_waited_are_let_go_when_no_rule_selects_the_event_any_more() {
    let mut net = raising(&["a", "b"]);
    for n in 0..40 {
        net.nodes[n % 2].set_for(&format!("k{n}"), "v", 10);
    }
    net.settle();
    net.advance_ms(11_000);
    for node in &net.nodes {
        assert!(node.sweep(0).is_empty());
        assert_eq!(node.expiries(), (0, 40));
        node.store.set_expiry_events(false);
        assert!(node.sweep(usize::MAX).is_empty());
        assert_eq!(node.expiries(), (40, 0));
        assert_eq!((node.keys(), node.bytes()), (0, 0));
    }
}

/// A member that has just started holds nothing, and would raise nothing: the others
/// do not pick it until its copy has been compared with theirs.
#[test]
fn a_member_that_has_just_started_is_not_picked_until_its_copy_was_compared() {
    let ids = ["a", "b", "c"];
    let mut net = raising(&ids);
    for n in 0..90 {
        net.nodes[n % 3].set_for(&format!("k{n}"), "v", 10);
    }
    net.settle();
    // `c` restarts: a new store under its name, linked, and not yet heard from.
    net.disconnect(0, 2);
    net.disconnect(1, 2);
    net.nodes[2] = node("c");
    net.nodes[2].store.set_expiry_events(true);
    net.nodes[2]
        .clock
        .store(net.nodes[0].clock.load(Relaxed), Relaxed);
    net.connect(0, 2);
    net.connect(1, 2);
    net.advance_ms(11_000);
    let mut raised: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for node in &net.nodes {
        for (key, _) in node.sweep(usize::MAX) {
            raised.entry(key).or_default().push(node.id.clone());
        }
    }
    assert_eq!(
        raised.len(),
        90,
        "none is left to the member that holds nothing"
    );
    for (key, by) in &raised {
        assert_eq!(by, &[picked(&["a", "b"], key)], "{key}");
    }
    // Compared: from here on it is one of the three.
    net.settle();
    net.repair();
    for n in 0..90 {
        net.nodes[n % 3].set_for(&format!("j{n}"), "v", 10);
    }
    net.settle();
    net.advance_ms(11_000);
    let raised = net.sweep_all();
    assert_eq!(raised.len(), 90);
    for (key, by) in &raised {
        assert_eq!(by, &[picked(&ids, key)], "{key}");
    }
}

/// D7's "bounded, late rather than lost", in a cluster: a mass expiry is handed over
/// at each node's pace, a tenth of a second's worth at a time, in order of expiry to
/// the second, each key once across the members, none shed; and the pace is one for
/// the node, across its `local` and its `eventual` namespaces.
#[test]
fn a_mass_expiry_is_handed_over_at_each_nodes_pace_in_order_and_none_is_shed() {
    const PER_SEC: u32 = 200;
    let ids = ["a", "b", "c"];
    let mut net = raising(&ids);
    // A `local` namespace on each node as well, whose keys expire with the rest.
    let locals: Vec<_> = net
        .nodes
        .iter()
        .map(|node| {
            let local = node
                .store
                .open("own", &NamespaceSettings::defaults(64 << 20))
                .unwrap();
            for n in 0..100 {
                let value = Value::Int(n);
                local
                    .call(&StateOp::Set {
                        key: format!("own{n}").as_bytes(),
                        value: stored(&value),
                        ttl: Some(10 + u64::try_from(n % 3).unwrap()),
                    })
                    .unwrap();
            }
            local
        })
        .collect();
    for n in 0..1_500 {
        net.nodes[n % 3].set_for(&format!("k{n}"), "v", 10 + (n % 3) as u64);
    }
    net.settle();
    net.advance_ms(15_000);

    let mut paces: Vec<Pace> = ids.iter().map(|_| Pace::new(PER_SEC)).collect();
    let mut raised: BTreeMap<String, usize> = BTreeMap::new();
    let mut own = [0usize; 3];
    let mut passes = 0;
    loop {
        let mut any = false;
        for (at, node) in net.nodes.iter().enumerate() {
            let now_ms = node.store.now_ms();
            let allowed = paces[at].allow(now_ms);
            let swept = node.store.sweep(allowed);
            paces[at].spend(swept.expired.len());
            assert!(
                swept.expired.len() <= usize::try_from(PER_SEC / 10).unwrap(),
                "{}: {} events in a tenth of a second",
                node.id,
                swept.expired.len()
            );
            let seconds: Vec<u64> = swept
                .expired
                .iter()
                .map(|expired| expired.expired_ms.div_ceil(1000))
                .collect();
            assert!(seconds.is_sorted(), "{}: {seconds:?}", node.id);
            for expired in &swept.expired {
                if &*expired.namespace == "own" {
                    own[at] += 1;
                } else {
                    *raised
                        .entry(String::from_utf8_lossy(&expired.key).into_owned())
                        .or_default() += 1;
                }
            }
            any |= swept.more || !swept.expired.is_empty();
        }
        net.settle();
        if !any {
            break;
        }
        passes += 1;
        net.advance_ms(100);
        assert!(passes < 1_000, "the backlog never emptied");
    }
    assert_eq!(raised.len(), 1_500, "none shed");
    assert!(raised.values().all(|times| *times == 1), "each once");
    assert_eq!(own, [100; 3], "and each node's own keys");
    // 1,800 events at 3 x 200 a second: three seconds at the least, and the node
    // with the largest share a little longer.
    assert!((30..45).contains(&passes), "{passes} passes");
    for node in &net.nodes {
        assert_eq!(node.keys(), 0, "{}", node.id);
        assert_eq!(node.expiries(), (1_500, 0), "{}", node.id);
    }
    drop(locals);
}

/// A replayed sequence is one node, and raises the events of an `eventual`
/// namespace's keys itself: [`Store::take_expired`] finds the key wherever it is.
#[test]
fn one_node_takes_an_expired_key_of_an_eventual_namespace_for_its_event() {
    let one = node("a");
    one.store.set_expiry_events(true);
    one.set_for("plain", "v", 10);
    one.set("kept", "v");
    one.del("kept");
    one.set_for("kept", "again", 10);
    one.set_for("found", "v", 10);
    one.advance_ms(11_000);
    // One the sweep has already found.
    let value = Value::from("w");
    one.call(&StateOp::Set {
        key: b"found",
        value: stored(&value),
        ttl: Some(100),
    })
    .unwrap();
    for (key, held) in [("plain", "v"), ("kept", "again"), ("found", "v")] {
        let expired = one.store.take_expired(NS, key.as_bytes()).unwrap();
        assert_eq!(
            expired.value.to_json().unwrap(),
            format!("\"{held}\""),
            "{key}"
        );
        assert_eq!(expired.kind, "lww");
        assert!(one.store.take_expired(NS, key.as_bytes()).is_none());
    }
    assert!(one.store.take_expired(NS, b"never").is_none());
    assert!(one.sweep(usize::MAX).is_empty());
    assert_eq!(one.get("found"), "\"w\"");
    // `Store::expire_now` is a `local` namespace's: it leaves this one's keys alone.
    assert!(!one.store.expire_now(NS, b"found"));
    assert_eq!(one.get("found"), "\"w\"");
}
