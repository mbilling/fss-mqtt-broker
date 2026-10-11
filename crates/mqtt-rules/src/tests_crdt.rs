//! State that merges (ADR 0085 D4, D6; delivery task T2): the merge is a join over
//! every type and every pair of concurrent operations, replicas converge from any
//! delivery order, the clock survives a wall clock that steps back or a peer that is
//! ahead, the sketch counts within its error, and the replicated form round-trips every
//! value and refuses everything else.
//!
//! The generators are deterministic (a seeded `SplitMix64`), so a failure names its seed
//! and repeats.

use std::sync::Arc;

use bytes::Bytes;

use super::*;
use crate::value::json_decode;

/// `SplitMix64`: a seed in, the same sequence out, everywhere.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).unwrap()).unwrap()
    }

    fn pick<'a, T>(&mut self, of: &'a [T]) -> &'a T {
        &of[self.below(of.len())]
    }
}

const T0: u64 = 1_800_000_000_000;

fn json(text: &str) -> Value {
    json_decode(text.as_bytes()).unwrap()
}

fn bin(bytes: &[u8]) -> Value {
    Value::Bin(Bytes::copy_from_slice(bytes))
}

/// Values of every kind a rule can hold, the awkward ones included.
fn values() -> Vec<Value> {
    vec![
        Value::Int(0),
        Value::Int(1),
        Value::Int(-7),
        Value::Int(i64::MAX),
        Value::Int(i64::MIN),
        json("1.0"),
        json("0.1"),
        json("1.0e300"),
        json("-2.5"),
        // Past 64 bits, and well past.
        json("18446744073709551616"),
        json("-340282366920938463463374607431768211456123456789"),
        Value::Bool(true),
        Value::Bool(false),
        Value::Null,
        Value::from(""),
        Value::from("on"),
        Value::from("true"),
        Value::from("blåbærsyltetøy"),
        // Not UTF-8.
        bin(&[0xff, 0x00, 0xd2, b'a']),
        bin(&[0xc3]),
        json("[]"),
        json("[1,1.0,\"a\",[null,true],{\"k\":[]}]"),
        json("{}"),
        json("{\"b\":1,\"a\":{\"z\":18446744073709551616,\"y\":0.5}}"),
        json("{\"a\":{\"y\":0.5,\"z\":18446744073709551616},\"b\":1}"),
    ]
}

fn numbers() -> Vec<Value> {
    values().into_iter().filter(Value::is_number).collect()
}

fn encoded(state: &KeyState) -> Vec<u8> {
    state.encoded()
}

fn merged(a: &KeyState, b: &KeyState) -> KeyState {
    let mut out = a.clone();
    out.merge(b);
    out
}

/// One random write. Elements and values come from a small pool, so that concurrent
/// writes meet on equal values, equal elements and equal hashes.
fn write_of<'a>(rng: &mut Rng, pool: &'a [Value], numbers: &'a [Value]) -> Write<'a> {
    match rng.below(9) {
        0 | 1 => Write::Set(rng.pick(pool)),
        2 => Write::SetOnce(rng.pick(pool)),
        3 => Write::Extreme {
            value: rng.pick(numbers),
            largest: rng.below(2) == 0,
        },
        4 => Write::Incr(*rng.pick(&[1, 1, -1, 5, i64::MAX, i64::MIN])),
        5 => Write::Add(rng.pick(pool)),
        6 => Write::HllAdd(stable_hash(&rng.next().to_be_bytes()[..1])),
        _ => Write::Delete,
    }
}

fn ttl_of(rng: &mut Rng) -> Option<u64> {
    *rng.pick(&[None, None, Some(1), Some(60), Some(3600)])
}

/// States as replicas come to hold them: a few nodes writing the same key at clocks
/// that disagree, hearing of each other now and then.
fn reachable_states(seed: u64, count: usize) -> Vec<KeyState> {
    let mut rng = Rng(seed);
    let (pool, numbers) = (values(), numbers());
    let mut replicas: Vec<Replica> = ["a", "b", "c"].iter().map(|n| Replica::new(n)).collect();
    let mut states = Vec::new();
    let mut wall = T0;
    while states.len() < count {
        let at = rng.below(replicas.len());
        // Some writes share a millisecond, some are seconds apart, some a clock behind.
        wall += *rng.pick(&[0, 0, 1, 1_000, 61_000]);
        let skewed = wall - *rng.pick(&[0, 0, 500, 30_000]);
        let write = write_of(&mut rng, &pool, &numbers);
        let _ = replicas[at].write(b"k", &write, ttl_of(&mut rng), Some(600), skewed);
        if rng.below(3) == 0 {
            let from = rng.below(replicas.len());
            if let Some(state) = replicas[from].state(b"k").cloned() {
                // A refusal (the sender's clock too far ahead) leaves the copy as it was.
                let _ = replicas[at].receive(b"k", &state, skewed);
            }
        }
        states.push(replicas[at].state(b"k").cloned().unwrap());
    }
    states
}

/// States nobody wrote but a peer could send: every type under stamps that tie or
/// nearly do, with and without a delete and an expiry.
fn arbitrary_states(seed: u64, count: usize) -> Vec<KeyState> {
    let mut rng = Rng(seed);
    let (pool, numbers) = (values(), numbers());
    let stamp = |rng: &mut Rng| {
        Stamp::new(
            T0 + u64::try_from(rng.below(3)).unwrap(),
            u16::try_from(rng.below(2)).unwrap(),
            u64::try_from(rng.below(2)).unwrap(),
        )
    };
    (0..count)
        .map(|_| {
            let deleted = if rng.below(2) == 0 {
                Stamp::ZERO
            } else {
                stamp(&mut rng)
            };
            let data = match rng.below(8) {
                0 => None,
                1 => Some(Data::Lww {
                    stamp: stamp(&mut rng),
                    value: normal(rng.pick(&pool)).unwrap(),
                }),
                2 => Some(Data::First {
                    stamp: stamp(&mut rng),
                    value: normal(rng.pick(&pool)).unwrap(),
                }),
                3 => Some(Data::Max(rng.pick(&numbers).clone())),
                4 => Some(Data::Min(rng.pick(&numbers).clone())),
                5 => {
                    let mut counts = Counts::default();
                    for _ in 0..rng.below(4) {
                        let node = *rng.pick(&["a", "b", "c"]);
                        let _ = counts.add(node, *rng.pick(&[1, -1, 9, i64::MAX]));
                    }
                    Some(Data::Sum(counts))
                }
                6 => {
                    let mut elements = Elements::default();
                    for _ in 0..rng.below(5) {
                        let element = normal(rng.pick(&pool)).unwrap();
                        let _ = elements.insert(&term_key(&element), &element, Value::clone);
                    }
                    Some(Data::Union(elements))
                }
                _ => {
                    let mut sketch = Box::<Hll>::default();
                    for _ in 0..rng.below(6) {
                        sketch.add(stable_hash(&[u8::try_from(rng.below(8)).unwrap()]));
                    }
                    Some(Data::Hll(sketch))
                }
            };
            let expires = *rng.pick(&[0, T0 + 1_000, T0 + 2_000]);
            KeyState::new(deleted, data, expires)
        })
        .collect()
}

// -- the merge is a join ----------------------------------------------------------------

/// Commutative, associative and idempotent over every pair and triple of `states`.
fn assert_join_laws(states: &[KeyState], what: &str) {
    for (i, a) in states.iter().enumerate() {
        assert_eq!(
            encoded(&merged(a, a)),
            encoded(a),
            "{what}: idempotent, {i}"
        );
        for (j, b) in states.iter().enumerate() {
            let ab = merged(a, b);
            assert_eq!(
                encoded(&ab),
                encoded(&merged(b, a)),
                "{what}: commutative, {i} {j}"
            );
            // Merging what is already merged in changes nothing.
            assert_eq!(
                encoded(&merged(&ab, a)),
                encoded(&ab),
                "{what}: absorbs, {i} {j}"
            );
            for (k, c) in states.iter().enumerate() {
                assert_eq!(
                    encoded(&merged(&ab, c)),
                    encoded(&merged(a, &merged(b, c))),
                    "{what}: associative, {i} {j} {k}"
                );
            }
        }
    }
}

#[test]
fn merge_is_a_join_over_states_replicas_reach() {
    for seed in 0..12 {
        assert_join_laws(&reachable_states(seed, 28), &format!("seed {seed}"));
    }
}

#[test]
fn merge_is_a_join_over_any_states_a_peer_could_send() {
    for seed in 100..112 {
        assert_join_laws(&arbitrary_states(seed, 28), &format!("seed {seed}"));
    }
}

#[test]
fn what_a_merge_gives_is_a_state_the_decoder_accepts() {
    let mut states = reachable_states(7, 40);
    states.extend(arbitrary_states(8, 40));
    for a in &states {
        for b in &states {
            let bytes = encoded(&merged(a, b));
            assert!(bytes.len() <= MAX_ENCODED_BYTES);
            let back = KeyState::decode(&bytes).expect("a merged state decodes");
            assert_eq!(encoded(&back), bytes);
        }
    }
}

// -- replicas converge ------------------------------------------------------------------

/// Hand every state of every replica to every other until nothing is left to hand
/// over, in an order the generator chooses.
fn exchange_all(replicas: &mut [Replica], rng: &mut Rng, wall: u64) {
    for _ in 0..2 {
        let mut pairs: Vec<(usize, usize)> = (0..replicas.len())
            .flat_map(|from| (0..replicas.len()).map(move |to| (from, to)))
            .filter(|(from, to)| from != to)
            .collect();
        while !pairs.is_empty() {
            let (from, to) = pairs.swap_remove(rng.below(pairs.len()));
            let states: Vec<(Vec<u8>, KeyState)> = replicas[from]
                .states()
                .map(|(key, state)| (key.to_vec(), state.clone()))
                .collect();
            for (key, state) in states {
                replicas[to].receive(&key, &state, wall).unwrap();
            }
        }
    }
}

fn assert_converged(replicas: &[Replica], wall: u64, what: &str) {
    let first: Vec<(Vec<u8>, Vec<u8>)> = replicas[0]
        .states()
        .map(|(key, state)| (key.to_vec(), encoded(state)))
        .collect();
    for replica in &replicas[1..] {
        let other: Vec<(Vec<u8>, Vec<u8>)> = replica
            .states()
            .map(|(key, state)| (key.to_vec(), encoded(state)))
            .collect();
        assert_eq!(first, other, "{what}");
        for (key, _) in &first {
            let (a, b) = (replicas[0].read(key, wall), replica.read(key, wall));
            assert_eq!(a.is_some(), b.is_some(), "{what}");
            if let (Some(a), Some(b)) = (a, b) {
                assert!(a.exact_eq(&b), "{what}: {a:?} and {b:?}");
            }
        }
    }
}

#[test]
fn replicas_converge_from_any_interleaving_of_writes_deletes_and_deliveries() {
    let (pool, numbers) = (values(), numbers());
    let keys: [&[u8]; 3] = [b"a", b"b", b"c"];
    for seed in 0..300 {
        let mut rng = Rng(seed);
        let mut replicas: Vec<Replica> =
            ["n1", "n2", "n3"].iter().map(|n| Replica::new(n)).collect();
        let mut wall = T0;
        for _ in 0..60 {
            wall += *rng.pick(&[0, 0, 1, 250, 2_000, 61_000]);
            let at = rng.below(replicas.len());
            // Each node's wall clock is its own: some behind, one sometimes stepping back.
            let seen = wall - *rng.pick(&[0, 0, 3, 900, 20_000]);
            if rng.below(5) < 3 {
                let write = write_of(&mut rng, &pool, &numbers);
                let key = *rng.pick(&keys);
                let _ = replicas[at].write(key, &write, ttl_of(&mut rng), Some(600), seen);
            } else {
                // One key's state, to one other node; sometimes twice.
                let from = rng.below(replicas.len());
                let key = *rng.pick(&keys);
                if let Some(state) = replicas[from].state(key).cloned() {
                    for _ in 0..=rng.below(2) {
                        let _ = replicas[at].receive(key, &state, seen);
                    }
                }
            }
        }
        // After the partition: every clock within the bound of every stamp.
        let wall = wall + MAX_FORWARD_SKEW_MS;
        exchange_all(&mut replicas, &mut rng, wall);
        assert_converged(&replicas, wall, &format!("seed {seed}"));
    }
}

/// Every write there is, once with and once without a TTL.
fn every_write<'a>(value: &'a Value, number: &'a Value) -> Vec<(Write<'a>, Option<u64>)> {
    let writes = [
        Write::Set(value),
        Write::SetOnce(value),
        Write::Extreme {
            value: number,
            largest: true,
        },
        Write::Extreme {
            value: number,
            largest: false,
        },
        Write::Incr(3),
        Write::Add(value),
        Write::HllAdd(stable_hash(b"x")),
        Write::Delete,
    ];
    writes
        .into_iter()
        .flat_map(|write| [(write, None), (write, Some(30))])
        .collect()
}

#[test]
fn every_pair_of_concurrent_operations_converges_whatever_the_key_held() {
    let (one, two) = (Value::from("one"), Value::from("two"));
    let (small, large) = (Value::Int(2), json("2.5"));
    let before = every_write(&one, &small);
    let (left, right) = (every_write(&one, &small), every_write(&two, &large));
    let mut cases = 0;
    // What the key held when the two nodes parted: nothing, or one of each type.
    for prior in std::iter::once(None).chain(before.iter().map(Some)) {
        for (a_write, a_ttl) in &left {
            for (b_write, b_ttl) in &right {
                // The same instant on both wall clocks (a tie the node decides), and
                // either node first.
                for (a_wall, b_wall) in [(T0 + 5, T0 + 5), (T0 + 5, T0 + 9), (T0 + 9, T0 + 5)] {
                    let mut a = Replica::new("a");
                    let mut b = Replica::new("b");
                    if let Some((write, ttl)) = prior {
                        a.write(b"k", write, *ttl, Some(600), T0).unwrap();
                        let state = a.state(b"k").cloned().unwrap();
                        b.receive(b"k", &state, T0).unwrap();
                    }
                    // A wrong type is refused on the node; nothing is written.
                    let _ = a.write(b"k", a_write, *a_ttl, Some(600), a_wall);
                    let _ = b.write(b"k", b_write, *b_ttl, Some(600), b_wall);
                    let (sa, sb) = (
                        a.state(b"k").cloned().unwrap_or_default(),
                        b.state(b"k").cloned().unwrap_or_default(),
                    );
                    // A third node hears both, in either order.
                    let (mut c, mut d) = (Replica::new("c"), Replica::new("d"));
                    c.receive(b"k", &sa, T0 + 9).unwrap();
                    c.receive(b"k", &sb, T0 + 9).unwrap();
                    d.receive(b"k", &sb, T0 + 9).unwrap();
                    d.receive(b"k", &sa, T0 + 9).unwrap();
                    a.receive(b"k", &sb, T0 + 9).unwrap();
                    b.receive(b"k", &sa, T0 + 9).unwrap();
                    let all = [a, b, c, d];
                    assert_converged(&all, T0 + 9, &format!("{prior:?} {a_write:?} {b_write:?}"));
                    cases += 1;
                }
            }
        }
    }
    assert_eq!(cases, 17 * 16 * 16 * 3);
}

// -- what each type resolves to ---------------------------------------------------------

fn read(replica: &Replica, wall: u64) -> Option<String> {
    replica
        .read(b"k", wall)
        .map(|value| value.to_json().unwrap())
}

fn hand(from: &Replica, to: &mut Replica, wall: u64) {
    let state = from.state(b"k").cloned().unwrap();
    to.receive(b"k", &state, wall).unwrap();
}

fn set(replica: &mut Replica, text: &str, wall: u64) {
    let value = Value::from(text);
    replica
        .write(b"k", &Write::Set(&value), None, Some(600), wall)
        .unwrap();
}

#[test]
fn a_causally_later_lww_write_is_never_lost_whatever_the_wall_clocks_say() {
    for behind in [0, 1, 999, 59_000, 10_000_000] {
        let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
        set(&mut a, "first", T0 + 60_000);
        // `b` has seen `a`'s write, and its own wall clock is behind `a`'s.
        hand(&a, &mut b, T0 + 60_000);
        set(&mut b, "second", (T0 + 60_000) - behind);
        hand(&b, &mut a, T0 + 60_000);
        assert_eq!(read(&a, T0 + 60_000).as_deref(), Some("\"second\""));
        assert_eq!(read(&b, T0 + 60_000).as_deref(), Some("\"second\""));
        // And a third write on `a`, whose clock now stands still.
        set(&mut a, "third", T0 + 60_000);
        hand(&a, &mut b, T0 + 60_000);
        assert_eq!(read(&b, T0 + 60_000).as_deref(), Some("\"third\""));
    }
}

#[test]
fn of_two_concurrent_lww_writes_the_later_stamp_wins_and_a_tie_goes_to_one_node() {
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    set(&mut a, "early", T0);
    set(&mut b, "late", T0 + 1);
    hand(&a, &mut b, T0 + 1);
    hand(&b, &mut a, T0 + 1);
    assert_eq!(read(&a, T0 + 1).as_deref(), Some("\"late\""));
    assert_eq!(read(&b, T0 + 1).as_deref(), Some("\"late\""));

    // The same millisecond and counter: the nodes' tags decide, the same way on both.
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    set(&mut a, "from a", T0);
    set(&mut b, "from b", T0);
    let (sa, sb) = (
        a.state(b"k").cloned().unwrap(),
        b.state(b"k").cloned().unwrap(),
    );
    assert_eq!(
        (sa.latest().ms(), sa.latest().counter()),
        (sb.latest().ms(), sb.latest().counter())
    );
    let winner = if sa.latest() > sb.latest() {
        "\"from a\""
    } else {
        "\"from b\""
    };
    hand(&a, &mut b, T0);
    hand(&b, &mut a, T0);
    assert_eq!(read(&a, T0).as_deref(), Some(winner));
    assert_eq!(read(&b, T0).as_deref(), Some(winner));
}

#[test]
fn a_delete_covers_exactly_the_lww_writes_stamped_before_it() {
    // Three nodes that have not heard of each other: a write before the delete, the
    // delete, a write after it.
    for (write_at, survives) in [(T0 + 4, false), (T0 + 6, true)] {
        let (mut writer, mut deleter) = (Replica::new("w"), Replica::new("d"));
        set(&mut writer, "written", write_at);
        deleter
            .write(b"k", &Write::Delete, None, None, T0 + 5)
            .unwrap();
        for (first, second) in [(&writer, &deleter), (&deleter, &writer)] {
            let mut third = Replica::new("t");
            hand(first, &mut third, T0 + 10);
            hand(second, &mut third, T0 + 10);
            assert_eq!(read(&third, T0 + 10).is_some(), survives, "{write_at}");
        }
    }
    // The same millisecond: the delete's stamp against the write's, tag included.
    let (mut writer, mut deleter) = (Replica::new("w"), Replica::new("d"));
    set(&mut writer, "written", T0);
    deleter.write(b"k", &Write::Delete, None, None, T0).unwrap();
    let after = writer.state(b"k").unwrap().latest() > deleter.state(b"k").unwrap().deleted();
    hand(&deleter, &mut writer, T0);
    assert_eq!(read(&writer, T0).is_some(), after);
}

#[test]
fn a_delete_is_not_undone_by_a_copy_from_before_it() {
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    for write in [
        Write::Set(&Value::Int(1)),
        Write::SetOnce(&Value::Int(1)),
        Write::Incr(1),
        Write::Add(&Value::Int(1)),
        Write::HllAdd(7),
        Write::Extreme {
            value: &Value::Int(1),
            largest: true,
        },
    ] {
        a.write(b"k", &write, None, Some(600), T0).unwrap();
        let stale = a.state(b"k").cloned().unwrap();
        hand(&a, &mut b, T0);
        a.write(b"k", &Write::Delete, None, None, T0 + 1).unwrap();
        // The late copy arrives after the delete, on the node that deleted and on one
        // that then hears of the delete.
        a.receive(b"k", &stale, T0 + 2).unwrap();
        assert_eq!(read(&a, T0 + 2), None, "{write:?}");
        hand(&a, &mut b, T0 + 2);
        b.receive(b"k", &stale, T0 + 2).unwrap();
        assert_eq!(read(&b, T0 + 2), None, "{write:?}");
    }
}

#[test]
fn first_keeps_the_earliest_write_and_starts_again_after_a_delete() {
    let once = |replica: &mut Replica, text: &str, wall: u64| {
        let value = Value::from(text);
        replica
            .write(b"k", &Write::SetOnce(&value), None, Some(600), wall)
            .unwrap();
    };
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    once(&mut a, "later", T0 + 3);
    once(&mut b, "earlier", T0 + 1);
    hand(&a, &mut b, T0 + 3);
    hand(&b, &mut a, T0 + 3);
    assert_eq!(read(&a, T0 + 3).as_deref(), Some("\"earlier\""));
    assert_eq!(read(&b, T0 + 3).as_deref(), Some("\"earlier\""));
    // Held: a second write changes nothing.
    once(&mut a, "ignored", T0 + 4);
    assert_eq!(read(&a, T0 + 4).as_deref(), Some("\"earlier\""));
    // Deleted, then written by a node that has seen the delete: the new write holds,
    // and the old one does not come back.
    a.write(b"k", &Write::Delete, None, None, T0 + 5).unwrap();
    hand(&a, &mut b, T0 + 5);
    once(&mut b, "again", T0 + 6);
    hand(&b, &mut a, T0 + 6);
    assert_eq!(read(&a, T0 + 6).as_deref(), Some("\"again\""));
}

#[test]
fn a_delete_covers_what_a_node_that_had_not_seen_it_wrote_to_an_accumulating_key() {
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    a.write(b"k", &Write::Incr(10), None, Some(600), T0)
        .unwrap();
    hand(&a, &mut b, T0);
    // `a` deletes; `b`, which has not heard, counts on, stamped after the delete.
    a.write(b"k", &Write::Delete, None, None, T0 + 1).unwrap();
    b.write(b"k", &Write::Incr(1), None, Some(600), T0 + 2)
        .unwrap();
    hand(&a, &mut b, T0 + 2);
    hand(&b, &mut a, T0 + 2);
    assert_eq!(read(&a, T0 + 2), None);
    assert_eq!(read(&b, T0 + 2), None);
    // What is counted after hearing of the delete stays, on both.
    b.write(b"k", &Write::Incr(2), None, Some(600), T0 + 3)
        .unwrap();
    a.write(b"k", &Write::Incr(5), None, Some(600), T0 + 3)
        .unwrap();
    hand(&a, &mut b, T0 + 3);
    hand(&b, &mut a, T0 + 3);
    assert_eq!(read(&a, T0 + 3).as_deref(), Some("7"));
    assert_eq!(read(&b, T0 + 3).as_deref(), Some("7"));
}

#[test]
fn a_counter_is_the_sum_of_every_nodes_increments_and_decrements() {
    let mut replicas: Vec<Replica> = ["a", "b", "c"].iter().map(|n| Replica::new(n)).collect();
    let mut rng = Rng(42);
    let mut expected = 0i64;
    for _ in 0..200 {
        let by = *rng.pick(&[1, 1, 2, -1, -3, 100]);
        expected += by;
        let at = rng.below(3);
        replicas[at]
            .write(b"k", &Write::Incr(by), None, Some(600), T0)
            .unwrap();
        if rng.below(4) == 0 {
            let (from, to) = (rng.below(3), rng.below(3));
            let state = replicas[from].state(b"k").cloned();
            if let Some(state) = state {
                replicas[to].receive(b"k", &state, T0).unwrap();
            }
        }
    }
    exchange_all(&mut replicas, &mut rng, T0);
    for replica in &replicas {
        assert_eq!(read(replica, T0), Some(expected.to_string()));
    }
}

#[test]
fn a_counter_refuses_what_would_pass_64_bits_and_reads_exactly_when_merged_past_them() {
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    for replica in [&mut a, &mut b] {
        replica
            .write(b"k", &Write::Incr(i64::MAX), None, Some(600), T0)
            .unwrap();
        assert_eq!(
            replica.write(b"k", &Write::Incr(1), None, Some(600), T0),
            Err(WriteError::Counter(CountsError::Overflow))
        );
        assert_eq!(read(replica, T0), Some(i64::MAX.to_string()));
    }
    // Each stayed within 64 bits; together they are past them, and the count is exact.
    hand(&a, &mut b, T0);
    assert_eq!(read(&b, T0).as_deref(), Some("18446744073709551614"));
    assert_eq!(
        b.write(b"k", &Write::Incr(-1), None, Some(600), T0),
        Err(WriteError::Counter(CountsError::Overflow))
    );
    // Up and down by the largest step, for ever: one node's two numbers are 128 bits.
    let mut c = Replica::new("c");
    for _ in 0..1_000 {
        c.write(b"n", &Write::Incr(i64::MAX), None, None, T0)
            .unwrap();
        c.write(b"n", &Write::Incr(-i64::MAX), None, None, T0)
            .unwrap();
    }
    assert_eq!(c.read(b"n", T0).unwrap().to_json().unwrap(), "0");
}

#[test]
fn a_counter_keeps_counts_of_a_bounded_number_of_nodes() {
    let mut counts = Counts::default();
    for i in 0..MAX_COUNTER_NODES {
        counts.add(&format!("node-{i:04}"), 1).unwrap();
    }
    assert_eq!(counts.add("zz-one-more", 1), Err(CountsError::TooManyNodes));
    assert_eq!(
        Counts::default().add(&"n".repeat(MAX_NODE_ID_BYTES + 1), 1),
        Err(CountsError::NodeId)
    );
    assert_eq!(counts.add("node-0000", 1), Ok(257));
    // Merged with a counter of other nodes: the first in order of id, on both sides.
    let mut other = Counts::default();
    for i in 0..MAX_COUNTER_NODES {
        other.add(&format!("m-{i:04}"), 2).unwrap();
    }
    let (mut left, mut right) = (counts.clone(), other.clone());
    left.merge(&other);
    right.merge(&counts);
    assert_eq!(left, right);
    assert_eq!(left.nodes(), MAX_COUNTER_NODES);
    assert_eq!(left.total_i64(), Some(512));
}

#[test]
fn max_and_min_keep_the_extreme_of_every_nodes_numbers() {
    for largest in [true, false] {
        let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
        for (replica, numbers) in [(&mut a, ["3", "-1.5"]), (&mut b, ["2.5", "40"])] {
            for number in numbers {
                let value = json(number);
                let write = Write::Extreme {
                    value: &value,
                    largest,
                };
                replica.write(b"k", &write, None, Some(600), T0).unwrap();
            }
        }
        hand(&a, &mut b, T0);
        hand(&b, &mut a, T0);
        let expected = if largest { "40" } else { "-1.5" };
        assert_eq!(read(&a, T0).as_deref(), Some(expected));
        assert_eq!(read(&b, T0).as_deref(), Some(expected));
    }
    // `1` and `1.0` are one number and two values: every node keeps the same one.
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    let (int, float) = (Value::Int(1), json("1.0"));
    let max = |value| Write::Extreme {
        value,
        largest: true,
    };
    a.write(b"k", &max(&int), None, Some(600), T0).unwrap();
    b.write(b"k", &max(&float), None, Some(600), T0).unwrap();
    hand(&a, &mut b, T0);
    hand(&b, &mut a, T0);
    // The integer, for `max` as for `min`: merged, it is not replaced by its float.
    assert_eq!(read(&a, T0).as_deref(), Some("1"));
    assert_eq!(read(&b, T0).as_deref(), Some("1"));
    let min = |value| Write::Extreme {
        value,
        largest: false,
    };
    a.write(b"m", &min(&float), None, Some(600), T0).unwrap();
    b.write(b"m", &min(&int), None, Some(600), T0).unwrap();
    let (sa, sb) = (
        a.state(b"m").cloned().unwrap(),
        b.state(b"m").cloned().unwrap(),
    );
    a.receive(b"m", &sb, T0).unwrap();
    b.receive(b"m", &sa, T0).unwrap();
    assert_eq!(a.read(b"m", T0).unwrap().to_json().unwrap(), "1");
    assert_eq!(b.read(b"m", T0).unwrap().to_json().unwrap(), "1");
    // On one node the number held stays when an equal one is offered, whichever came
    // first: a write replaces only what it is strictly beyond.
    let mut c = Replica::new("c");
    c.write(b"k", &max(&float), None, Some(600), T0).unwrap();
    c.write(b"k", &max(&int), None, Some(600), T0).unwrap();
    assert_eq!(read(&c, T0).as_deref(), Some("1.0"));
    // Not a number: refused.
    assert!(matches!(
        a.write(b"k", &max(&Value::from("9")), None, None, T0),
        Err(WriteError::Value(_))
    ));
}

#[test]
fn a_set_is_the_union_in_term_order_and_holds_at_most_1024_elements() {
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    for (replica, elements) in [
        (&mut a, ["\"b\"", "1", "{\"x\":1}", "[2]"]),
        (&mut b, ["1.0", "\"a\"", "{\"y\":1}", "1"]),
    ] {
        for element in elements {
            let value = json(element);
            replica
                .write(b"k", &Write::Add(&value), None, Some(600), T0)
                .unwrap();
        }
    }
    hand(&a, &mut b, T0);
    hand(&b, &mut a, T0);
    // Numbers, then maps, then lists, then text; `1` and `1.0` are two elements, and
    // two maps of one size are in one order on every node.
    let expected = read(&a, T0).unwrap();
    assert_eq!(read(&b, T0).unwrap(), expected);
    assert!(expected.ends_with(",[2],\"a\",\"b\"]"), "{expected}");
    assert!(
        expected.starts_with("[1,1.0,{") || expected.starts_with("[1.0,1,{"),
        "{expected}"
    );
    assert!(expected.contains("{\"x\":1}") && expected.contains("{\"y\":1}"));

    // Full on each side with different elements: the union keeps the first 1,024.
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    for i in 0..1024 {
        let (even, odd) = (Value::Int(2 * i), Value::Int(2 * i + 1));
        a.write(b"k", &Write::Add(&even), None, Some(600), T0)
            .unwrap();
        b.write(b"k", &Write::Add(&odd), None, Some(600), T0)
            .unwrap();
    }
    assert_eq!(
        a.write(b"k", &Write::Add(&Value::Int(-1)), None, None, T0),
        Err(WriteError::SetFull)
    );
    hand(&a, &mut b, T0);
    hand(&b, &mut a, T0);
    let Some(Data::Union(elements)) = a.state(b"k").unwrap().data() else {
        panic!("a set");
    };
    assert_eq!(elements.len(), 1024);
    assert!(elements
        .iter()
        .zip(0..)
        .all(|(element, i)| element.exact_eq(&Value::Int(i))));
    assert_eq!(
        encoded(a.state(b"k").unwrap()),
        encoded(b.state(b"k").unwrap())
    );
}

#[test]
fn two_types_on_one_key_resolve_to_one_and_a_local_write_of_another_type_is_refused() {
    let one = Value::Int(1);
    let (mut a, mut b, mut c) = (Replica::new("a"), Replica::new("b"), Replica::new("c"));
    a.write(b"k", &Write::Incr(1), None, Some(600), T0).unwrap();
    assert_eq!(
        a.write(b"k", &Write::Set(&one), None, None, T0),
        Err(WriteError::WrongType { held: "sum" })
    );
    // Created at once as a counter, a set and a plain value: the plain value wins...
    b.write(b"k", &Write::Add(&one), None, Some(600), T0)
        .unwrap();
    c.write(b"k", &Write::Set(&one), None, Some(600), T0)
        .unwrap();
    let mut all = Replica::new("d");
    for from in [&a, &b, &c] {
        hand(from, &mut all, T0);
    }
    assert_eq!(all.state(b"k").unwrap().data().unwrap().type_name(), "lww");
    // ...and between the other two, the type later in D4's table.
    hand(&a, &mut b, T0);
    assert_eq!(b.state(b"k").unwrap().data().unwrap().type_name(), "union");
}

#[test]
fn an_expiry_travels_with_an_lww_write_and_is_the_later_of_two_otherwise() {
    let value = Value::Int(1);
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    a.write(b"k", &Write::Set(&value), Some(600), None, T0)
        .unwrap();
    b.write(b"k", &Write::Set(&value), Some(10), None, T0 + 1)
        .unwrap();
    hand(&a, &mut b, T0 + 1);
    hand(&b, &mut a, T0 + 1);
    // The later write's, though it is the shorter.
    assert_eq!(a.state(b"k").unwrap().expires_ms(), T0 + 1 + 10_000);
    assert_eq!(read(&a, T0 + 10_000), Some("1".to_string()));
    assert_eq!(read(&a, T0 + 10_001), None);

    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    a.write(b"k", &Write::Incr(1), Some(600), None, T0).unwrap();
    b.write(b"k", &Write::Incr(1), Some(10), None, T0 + 1)
        .unwrap();
    hand(&a, &mut b, T0 + 1);
    hand(&b, &mut a, T0 + 1);
    assert_eq!(a.state(b"k").unwrap().expires_ms(), T0 + 600_000);
    assert_eq!(b.state(b"k").unwrap().expires_ms(), T0 + 600_000);
    // A write without a TTL leaves it; a new key gets the default.
    a.write(b"k", &Write::Incr(1), None, Some(5), T0 + 2)
        .unwrap();
    assert_eq!(a.state(b"k").unwrap().expires_ms(), T0 + 600_000);
    a.write(b"new", &Write::Incr(1), None, Some(5), T0 + 2)
        .unwrap();
    assert_eq!(a.state(b"new").unwrap().expires_ms(), T0 + 2 + 5_000);
}

#[test]
fn a_key_written_after_it_expired_starts_afresh_on_every_node() {
    let (mut a, mut b) = (Replica::new("a"), Replica::new("b"));
    a.write(b"k", &Write::Incr(40), Some(10), None, T0).unwrap();
    hand(&a, &mut b, T0);
    b.write(b"k", &Write::Incr(2), None, None, T0 + 1).unwrap();
    assert_eq!(read(&b, T0 + 1).as_deref(), Some("42"));
    // Expired; `a` counts again, and `b`'s old copy must not add its 42 to it.
    assert_eq!(read(&a, T0 + 10_000), None);
    a.write(b"k", &Write::Incr(1), Some(10), None, T0 + 10_000)
        .unwrap();
    hand(&a, &mut b, T0 + 10_000);
    hand(&b, &mut a, T0 + 10_000);
    assert_eq!(read(&a, T0 + 10_000).as_deref(), Some("1"));
    assert_eq!(read(&b, T0 + 10_000).as_deref(), Some("1"));
    // The same for a value held once.
    let (first, second) = (Value::from("first"), Value::from("second"));
    let mut c = Replica::new("c");
    c.write(b"k", &Write::SetOnce(&first), Some(10), None, T0)
        .unwrap();
    c.write(b"k", &Write::SetOnce(&second), Some(10), None, T0 + 9_999)
        .unwrap();
    assert_eq!(read(&c, T0 + 9_999).as_deref(), Some("\"first\""));
    c.write(b"k", &Write::SetOnce(&second), None, Some(10), T0 + 19_999)
        .unwrap();
    assert_eq!(read(&c, T0 + 19_999).as_deref(), Some("\"second\""));
}

#[test]
fn tombstones_are_kept_for_the_window_and_then_forgotten() {
    const WINDOW: u64 = 3_600_000;
    let mut replica = Replica::new("a");
    set(&mut replica, "x", T0);
    replica
        .write(b"k", &Write::Delete, None, None, T0 + 1)
        .unwrap();
    assert_eq!(replica.forget(T0 + WINDOW, WINDOW), 0);
    assert!(replica.state(b"k").is_some());
    assert_eq!(replica.forget(T0 + 1 + WINDOW, WINDOW), 1);
    assert!(replica.state(b"k").is_none());

    // A key deleted every second for a day: what is held never passes one window's
    // worth, and a key that holds a value is never forgotten.
    let mut replica = Replica::new("a");
    let kept = Value::from("kept");
    replica
        .write(b"k", &Write::Set(&kept), None, None, T0)
        .unwrap();
    let mut most = 0;
    for second in 0..86_400u64 {
        let wall = T0 + second * 1_000;
        let key = second.to_be_bytes();
        replica
            .write(&key, &Write::Delete, None, None, wall)
            .unwrap();
        if second % 60 == 0 {
            replica.forget(wall, WINDOW);
            most = most.max(replica.states().count());
        }
    }
    assert!(most <= 3_600 + 60 + 1, "{most}");
    assert!(replica.state(b"k").is_some());
    // What has expired goes with its delete's window too, deleted or not.
    let mut replica = Replica::new("a");
    replica
        .write(b"k", &Write::Incr(1), Some(1), None, T0)
        .unwrap();
    assert_eq!(replica.forget(T0 + 999, WINDOW), 0);
    assert_eq!(replica.forget(T0 + 1_000, WINDOW), 1);
}

// -- the clock --------------------------------------------------------------------------

#[test]
fn the_clock_follows_the_wall_clock_and_never_goes_back_when_it_does() {
    let clock = Hlc::new("a");
    let first = clock.now(T0);
    assert_eq!((first.ms(), first.counter()), (T0, 0));
    // The same millisecond: the counter.
    let second = clock.now(T0);
    assert_eq!((second.ms(), second.counter()), (T0, 1));
    // The wall clock steps back an hour: stamps keep rising, and so does what
    // expiries are judged by.
    let mut last = second;
    for _ in 0..1_000 {
        let next = clock.now(T0 - 3_600_000);
        assert!(next > last);
        last = next;
    }
    assert_eq!(clock.physical_ms(T0 - 3_600_000), T0);
    // It catches up: the wall clock again.
    let next = clock.now(T0 + 5);
    assert_eq!((next.ms(), next.counter()), (T0 + 5, 0));
    assert!(next > last);
    assert_eq!(clock.physical_ms(T0 + 9), T0 + 9);
}

#[test]
fn the_counter_carries_into_the_milliseconds_when_one_millisecond_runs_out() {
    let clock = Hlc::new("a");
    let mut last = clock.now(T0);
    for _ in 0..70_000 {
        let next = clock.now(T0);
        assert!(next > last);
        last = next;
    }
    assert_eq!((last.ms(), last.counter()), (T0 + 1, 4_464));
}

#[test]
fn a_stamp_from_another_node_moves_the_clock_past_it() {
    let clock = Hlc::new("a");
    // The other node is 30 seconds ahead: within the bound.
    let seen = Stamp::new(T0 + 30_000, 7, 99);
    clock.observe(seen, T0).unwrap();
    let next = clock.now(T0);
    assert!(next > seen);
    assert_eq!((next.ms(), next.counter()), (T0 + 30_000, 8));
    assert_eq!(clock.physical_ms(T0), T0 + 30_000);
    // An older one changes nothing.
    clock.observe(Stamp::new(T0, 0, 99), T0).unwrap();
    assert_eq!(clock.now(T0).counter(), 9);
}

#[test]
fn a_stamp_too_far_ahead_is_refused_and_leaves_the_clock_alone() {
    let clock = Hlc::new("a");
    // Exactly the bound is accepted.
    clock
        .observe(Stamp::new(T0 + MAX_FORWARD_SKEW_MS, 0, 1), T0)
        .unwrap();
    let clock = Hlc::new("a");
    for ahead in [MAX_FORWARD_SKEW_MS + 1, 86_400_000, u64::MAX] {
        let seen = Stamp::new(T0.saturating_add(ahead), 0, 1);
        let refused = clock.observe(seen, T0).unwrap_err();
        assert_eq!(refused.ahead_ms, seen.ms() - T0);
        assert_eq!(clock.now(T0).ms(), T0);
    }
    // A replica refuses the whole state, and merges it once its clock has caught up.
    let (mut ahead, mut behind) = (Replica::new("a"), Replica::new("b"));
    set(&mut ahead, "from the future", T0 + 3_600_000);
    let state = ahead.state(b"k").cloned().unwrap();
    assert!(behind.receive(b"k", &state, T0).is_err());
    assert!(behind.state(b"k").is_none());
    set(&mut behind, "now", T0);
    assert_eq!(behind.state(b"k").unwrap().latest().ms(), T0);
    behind
        .receive(b"k", &state, T0 + 3_600_000 - MAX_FORWARD_SKEW_MS)
        .unwrap();
    assert_eq!(
        read(&behind, T0 + 3_600_000).as_deref(),
        Some("\"from the future\"")
    );
}

#[test]
fn stamps_taken_by_several_threads_at_once_are_all_different() {
    let clock = Arc::new(Hlc::new("a"));
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let clock = clock.clone();
            std::thread::spawn(move || {
                let mut stamps = Vec::with_capacity(20_000);
                for _ in 0..20_000 {
                    stamps.push(clock.now(T0));
                }
                assert!(stamps.windows(2).all(|pair| pair[0] < pair[1]));
                stamps
            })
        })
        .collect();
    let mut all: Vec<Stamp> = threads
        .into_iter()
        .flat_map(|thread| thread.join().unwrap())
        .collect();
    all.sort_unstable();
    all.dedup();
    assert_eq!(all.len(), 80_000);
}

// -- the hash and the sketch ------------------------------------------------------------

#[test]
fn the_hash_is_siphash_2_4_and_its_values_are_pinned() {
    // The reference implementation's vector: key 00..0f, input 00..0e.
    let input: Vec<u8> = (0..15).collect();
    assert_eq!(
        siphash24(0x0706_0504_0302_0100, 0x0f0e_0d0c_0b0a_0908, &input),
        0xa129_ca61_49be_45e5
    );
    // Under this crate's key. A change here changes every sketch and every tie:
    // sketches made by two releases would no longer merge.
    assert_eq!(stable_hash(b""), PINNED_HASHES[0]);
    assert_eq!(stable_hash(b"a"), PINNED_HASHES[1]);
    assert_eq!(stable_hash(b"device-0042"), PINNED_HASHES[2]);
    assert_eq!(stable_hash(&term_key(&Value::Int(1))), PINNED_HASHES[3]);
}

const PINNED_HASHES: [u64; 4] = [
    0x96d4_2926_18a5_8c6e,
    0x85a4_932a_edab_2b2f,
    0xb406_9ffc_3f0b_80df,
    0x8077_1137_53f7_ae59,
];

fn sketch_of(range: std::ops::Range<u64>) -> Hll {
    let mut sketch = Hll::default();
    for i in range {
        sketch.add(stable_hash(format!("device-{i}").as_bytes()));
    }
    sketch
}

#[test]
fn the_sketch_counts_within_its_error() {
    assert_eq!(Hll::default().count(), 0);
    // Few values: exact, or as good as.
    for n in [1u64, 2, 3, 10, 100] {
        assert_eq!(sketch_of(0..n).count(), n);
    }
    // 2^14 registers: a standard error of 1.04 / sqrt(16384) = 0.81%. Three of them.
    let mut worst = 0.0f64;
    let mut sum = 0.0f64;
    let cases = [
        1_000u32, 3_000, 5_000, 10_000, 20_000, 50_000, 100_000, 300_000,
    ];
    for n in cases {
        // A different set of values for each size.
        let from = u64::from(n) * 7;
        let got = sketch_of(from..from + u64::from(n)).count();
        let error = (f64::from(u32::try_from(got).unwrap()) - f64::from(n)).abs() / f64::from(n);
        assert!(error < 0.025, "{n}: estimated {got}");
        worst = worst.max(error);
        sum += error;
    }
    assert!(sum / 8.0 < 0.01, "mean error {}", sum / 8.0);
    assert!(worst > 0.0, "an estimate, not a count");
}

#[test]
fn adding_a_value_again_changes_nothing() {
    let mut sketch = sketch_of(0..5_000);
    let before = sketch.clone();
    for i in 0..5_000u64 {
        assert!(!sketch.add(stable_hash(format!("device-{i}").as_bytes())));
    }
    assert_eq!(sketch, before);
}

#[test]
fn merged_sketches_are_the_sketch_of_everything_either_saw() {
    // Sparse with sparse, sparse with dense, dense with dense, overlapping.
    for (left, right) in [
        (0..100u64, 50..200u64),
        (0..2_000, 1_000..3_500),
        (0..100, 0..50_000),
        (0..40_000, 30_000..90_000),
    ] {
        let whole = sketch_of(left.start.min(right.start)..left.end.max(right.end));
        let (a, b) = (sketch_of(left.clone()), sketch_of(right.clone()));
        let (mut ab, mut ba) = (a.clone(), b.clone());
        ab.merge(&b);
        ba.merge(&a);
        assert_eq!(ab, whole, "{left:?} {right:?}");
        assert_eq!(ba, whole, "{left:?} {right:?}");
        assert_eq!(ab.count(), whole.count());
    }
}

#[test]
fn a_sketch_is_a_list_of_registers_until_that_would_take_as_much_as_all_of_them() {
    let mut sketch = Hll::default();
    assert_eq!(sketch.bytes(), 0);
    let mut i = 0u64;
    while matches!(sketch.registers, Registers::Sparse(_)) {
        sketch.add(stable_hash(&i.to_be_bytes()));
        i += 1;
        assert!(sketch.bytes() <= HLL_DENSE_BYTES);
    }
    assert_eq!(HLL_DENSE_BYTES, 12 * 1024);
    assert_eq!(sketch.bytes(), HLL_DENSE_BYTES);
    assert_eq!(usize::from(sketch.histogram[0]), HLL_REGISTERS - 3_073);
    // Both forms round-trip, and the estimate does not depend on the form.
    for n in [0u64, 1, 3_000, 4_000, 200_000] {
        let state = KeyState::new(Stamp::ZERO, Some(Data::Hll(Box::new(sketch_of(0..n)))), 0);
        let back = KeyState::decode(&encoded(&state)).unwrap();
        let (Some(Data::Hll(a)), Some(Data::Hll(b))) = (state.data(), back.data()) else {
            panic!("a sketch");
        };
        assert_eq!(a, b);
        assert_eq!(a.count(), b.count());
    }
}

#[test]
fn a_sketchs_estimate_and_bytes_are_the_same_on_every_node_and_release() {
    // Pinned: the hash, the register a value lands in, the estimator's arithmetic and
    // the replicated form all show up here.
    let sketch = sketch_of(0..100_000);
    let state = KeyState::new(Stamp::ZERO, Some(Data::Hll(Box::new(sketch.clone()))), 0);
    assert_eq!(sketch.count(), PINNED_COUNT);
    assert_eq!(stable_hash(&encoded(&state)), PINNED_SKETCH);
}

const PINNED_COUNT: u64 = 100_064;
const PINNED_SKETCH: u64 = 0xf05b_9aa1_2109_9874;

// -- the replicated form ----------------------------------------------------------------

/// A state of every type holding `value` (where the type can).
fn states_holding(value: &Value) -> Vec<KeyState> {
    let stamp = Stamp::new(T0, 3, 9);
    let held = normal(value).unwrap();
    let mut states = vec![
        KeyState::new(
            Stamp::ZERO,
            Some(Data::Lww {
                stamp,
                value: held.clone(),
            }),
            T0 + 5,
        ),
        KeyState::new(
            Stamp::new(T0 - 1, 0, 2),
            Some(Data::First {
                stamp,
                value: held.clone(),
            }),
            0,
        ),
    ];
    if held.is_number() {
        states.push(KeyState::new(stamp, Some(Data::Max(held.clone())), 0));
        states.push(KeyState::new(stamp, Some(Data::Min(held.clone())), 1));
    }
    if crate::state::written_len(&held) <= MAX_ELEMENT_BYTES {
        let mut elements = Elements::default();
        elements
            .insert(&term_key(&held), &held, Value::clone)
            .unwrap();
        states.push(KeyState::new(stamp, Some(Data::Union(elements)), u64::MAX));
    }
    states
}

#[test]
fn the_replicated_form_round_trips_every_rule_value() {
    let mut all = values();
    all.push(Value::from("x".repeat(MAX_VALUE_BYTES)));
    all.push(bin(&[0x80; MAX_VALUE_BYTES]));
    for value in &all {
        for state in states_holding(value) {
            let bytes = encoded(&state);
            let back = KeyState::decode(&bytes).unwrap_or_else(|e| panic!("{value:?}: {e}"));
            assert_eq!(encoded(&back), bytes, "{value:?}");
            let read = match back.data().unwrap() {
                Data::Union(elements) => elements.iter().next().unwrap().clone(),
                data => data.read(),
            };
            // Exactly the value: an integer past 64 bits is that integer and not a
            // float, a float a float, bytes that are not UTF-8 the same bytes, a map's
            // keys in the order they were written.
            assert!(read.exact_eq(value), "{value:?} came back as {read:?}");
            assert_eq!(read.to_json().unwrap(), value.to_json().unwrap());
            assert_eq!(read.as_bytes(), value.as_bytes());
            assert_eq!(read.type_name(), normal(value).unwrap().type_name());
        }
    }
    // The header too.
    let state = KeyState::new(Stamp::new(T0, 65_535, u64::MAX), None, 0);
    let back = KeyState::decode(&encoded(&state)).unwrap();
    assert_eq!(back.deleted(), Stamp::new(T0, 65_535, u64::MAX));
    assert!(back.data().is_none());
}

#[test]
fn what_the_form_cannot_carry_is_settled_by_the_node_that_writes() {
    // ADR 0086 D3's form has no `-0.0`, no bytes that are not UTF-8 inside a container
    // and no `undefined`: `normal` is what every replica will hold, and it is stable.
    let negative_zero = Value::Float(-0.0);
    let nested = Value::from(vec![bin(&[0xff, b'a']), Value::Undefined]);
    for (value, becomes) in [
        (&negative_zero, "0.0"),
        (&nested, "[\"\u{fffd}a\",\"undefined\"]"),
    ] {
        let held = normal(value).unwrap();
        assert_eq!(held.to_json().unwrap(), becomes);
        let again = normal(&held).unwrap();
        assert!(again.exact_eq(&held));
        let mut replica = Replica::new("a");
        replica
            .write(b"k", &Write::Set(value), None, None, T0)
            .unwrap();
        let state = replica.state(b"k").unwrap();
        let back = KeyState::decode(&encoded(state)).unwrap();
        assert!(back.data().unwrap().read().exact_eq(&held));
    }
    assert!(normal(&Value::Undefined).is_err());
    let mut replica = Replica::new("a");
    assert!(matches!(
        replica.write(b"k", &Write::Set(&Value::Undefined), None, None, T0),
        Err(WriteError::Value(_))
    ));
    // Text is text, whichever way the rule held it.
    assert!(matches!(normal(&bin(b"abc")).unwrap(), Value::Str(_)));
}

/// The largest state of each type.
fn largest_states() -> Vec<KeyState> {
    let stamp = Stamp::new(MAX_MS, u16::MAX, u64::MAX);
    let mut elements = Elements::default();
    for i in 0..MAX_SET_ELEMENTS {
        // Not UTF-8, the largest an element may be, and each different.
        let mut bytes = vec![0x80; MAX_ELEMENT_BYTES - 4];
        bytes.extend_from_slice(&u32::try_from(i).unwrap().to_be_bytes());
        let element = bin(&bytes);
        elements
            .insert(&term_key(&element), &element, Value::clone)
            .unwrap();
    }
    let mut rows: Vec<Row> = (0..MAX_COUNTER_NODES)
        .map(|i| {
            let node = format!("{i:x>255}");
            assert_eq!(node.len(), MAX_NODE_ID_BYTES);
            Row {
                node: node.into_boxed_str(),
                up: u128::MAX,
                down: u128::MAX,
            }
        })
        .collect();
    rows.sort_by(|a, b| a.node.cmp(&b.node));
    let counts = Counts::from_rows(rows);
    let value = bin(&[0xff; MAX_VALUE_BYTES]);
    vec![
        KeyState::new(stamp, Some(Data::Union(elements)), u64::MAX),
        KeyState::new(stamp, Some(Data::Sum(counts)), u64::MAX),
        KeyState::new(
            stamp,
            Some(Data::Hll(Box::new(sketch_of(0..500_000)))),
            u64::MAX,
        ),
        KeyState::new(
            Stamp::ZERO,
            Some(Data::Lww {
                stamp,
                value: value.clone(),
            }),
            u64::MAX,
        ),
        KeyState::new(Stamp::ZERO, Some(Data::Max(json("1.0e300"))), u64::MAX),
    ]
}

#[test]
fn every_replicated_form_is_within_its_bound() {
    let sizes: Vec<usize> = largest_states()
        .iter()
        .map(|state| {
            let bytes = encoded(state);
            assert!(bytes.len() <= MAX_ENCODED_BYTES, "{}", bytes.len());
            let back = KeyState::decode(&bytes).expect("the largest state decodes");
            assert_eq!(encoded(&back), bytes);
            bytes.len()
        })
        .collect();
    // The bound is the full set's size, to the byte; a counter of every node and a
    // dense sketch are well inside it.
    assert_eq!(sizes[0], MAX_ENCODED_BYTES);
    assert_eq!(MAX_ENCODED_BYTES, 265_246);
    assert!(sizes[1] < 80_000 && sizes[2] < 12_400, "{sizes:?}");
    // One byte more is refused before anything is read.
    let mut long = encoded(&largest_states()[0]);
    long.push(0);
    assert_eq!(KeyState::decode(&long).unwrap_err(), DecodeError::TooLarge);
    // A counter of that many nodes still reads exactly.
    let Some(Data::Sum(counts)) = largest_states()[1].data().cloned() else {
        panic!("a counter");
    };
    assert_eq!(counts.total().to_json().unwrap(), "0");
}

#[test]
fn a_state_cut_short_anywhere_or_with_bytes_after_it_is_refused() {
    let mut states = reachable_states(3, 30);
    states.extend(arbitrary_states(4, 30));
    states.extend(largest_states().into_iter().skip(2));
    for state in &states {
        let bytes = encoded(state);
        // Every prefix of the larger ones would be slow, and proves nothing more.
        let step = (bytes.len() / 300).max(1);
        for cut in (0..bytes.len()).step_by(step) {
            assert!(KeyState::decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(KeyState::decode(&longer).is_err());
    }
}

#[test]
fn the_decoder_accepts_only_what_the_encoder_writes() {
    let stamp = |ms: u64| {
        let mut out = Vec::new();
        Stamp::new(ms, 0, 1).put(&mut out);
        out
    };
    let header = |deleted: u64, expires: u8, tag: u8| {
        let mut out = vec![VERSION];
        out.extend(stamp(deleted));
        out.extend([expires, tag]);
        out
    };
    let with = |mut head: Vec<u8>, rest: &[u8]| {
        head.extend_from_slice(rest);
        head
    };
    let lww = |value: &[u8]| with(with(header(0, 0, 1), &stamp(5)), value);
    for (what, bytes) in [
        ("nothing", vec![]),
        ("another version", with(vec![2], &header(0, 0, 0)[1..])),
        ("an unknown type", header(0, 0, 8)),
        ("an expiry with nothing to expire", header(0, 9, 0)),
        ("a write its own delete covers", {
            with(with(header(5, 0, 1), &stamp(5)), b"j\x011")
        }),
        ("an unknown value tag", lww(b"x\x011")),
        ("text as JSON", lww(b"j\x03\"a\"")),
        ("JSON with a space", lww(b"j\x02 1")),
        ("JSON that is not", lww(b"j\x01{")),
        ("a float written another way", lww(b"j\x041.00")),
        ("a length written long", lww(b"b\x81\x00a")),
        ("a value past its bound", lww(b"b\x81\x20")),
        (
            "a maximum that is no number",
            with(header(0, 0, 3), b"b\x01a"),
        ),
        (
            "a counter of too many nodes",
            with(header(0, 0, 5), &[0x81, 0x02]),
        ),
        ("a counter's nodes out of order", {
            with(header(0, 0, 5), b"\x02\x01b\x01\x00\x01a\x01\x00")
        }),
        ("a count past 128 bits", {
            with(with(header(0, 0, 5), b"\x01\x01a"), &[0xff; 19])
        }),
        ("a set of too many", with(header(0, 0, 6), &[0x81, 0x08])),
        (
            "a set out of order",
            with(header(0, 0, 6), b"\x02j\x012j\x011"),
        ),
        ("a set with one element twice", {
            with(header(0, 0, 6), b"\x02j\x011j\x011")
        }),
        ("an unknown sketch form", with(header(0, 0, 7), &[2])),
        (
            "a register of rank 0",
            with(header(0, 0, 7), &[0, 1, 0, 0, 0]),
        ),
        (
            "a register of rank 52",
            with(header(0, 0, 7), &[0, 1, 0, 0, 52]),
        ),
        (
            "a register past the last",
            with(header(0, 0, 7), &[0, 1, 0x40, 0, 1]),
        ),
        ("registers out of order", {
            with(header(0, 0, 7), &[0, 2, 0, 1, 1, 0, 1, 1])
        }),
        ("a dense sketch of nothing", {
            with(with(header(0, 0, 7), &[1]), &[0; HLL_DENSE_BYTES])
        }),
        ("a dense register past 51", {
            with(with(header(0, 0, 7), &[1]), &[0xff; HLL_DENSE_BYTES])
        }),
    ] {
        assert!(KeyState::decode(&bytes).is_err(), "{what}");
    }
    // And the smallest states it does accept.
    assert!(KeyState::decode(&header(0, 0, 0)).is_ok());
    assert!(KeyState::decode(&lww(b"j\x011")).is_ok());
    assert!(KeyState::decode(&with(header(0, 0, 5), b"\x01\x01a\x01\x00")).is_ok());
    assert!(KeyState::decode(&with(header(0, 0, 7), &[0, 1, 0, 0, 1])).is_ok());
}

#[test]
fn bytes_changed_at_random_are_a_canonical_state_or_an_error_and_never_a_panic() {
    let mut rng = Rng(2026);
    let mut states = reachable_states(5, 40);
    states.extend(arbitrary_states(6, 40));
    let mut accepted = 0;
    for state in &states {
        let bytes = encoded(state);
        for _ in 0..400 {
            let mut changed = bytes.clone();
            for _ in 0..=rng.below(3) {
                let at = rng.below(changed.len());
                match rng.below(3) {
                    0 => changed[at] ^= 1 << rng.below(8),
                    1 => changed[at] = u8::try_from(rng.below(256)).unwrap(),
                    _ => changed.truncate(at.max(1)),
                }
            }
            if let Ok(back) = KeyState::decode(&changed) {
                assert_eq!(encoded(&back), changed);
                assert_eq!(encoded(&merged(&back, &back)), changed);
                accepted += 1;
            }
        }
    }
    assert!(accepted > 100, "{accepted}");
}

#[test]
fn a_length_field_is_not_believed_past_the_bytes_that_are_there() {
    // A set that claims 1,024 elements, a counter that claims 256 nodes and a sketch
    // that claims 3,072 registers, each followed by nothing.
    for (tag, count) in [(6u8, [0x80u8, 0x08]), (5, [0x80, 0x02]), (7, [0x80, 0x18])] {
        let mut bytes = vec![VERSION];
        bytes.extend([0; 16]);
        bytes.extend([0, tag]);
        if tag == 7 {
            bytes.push(0);
        }
        bytes.extend(count);
        assert_eq!(
            KeyState::decode(&bytes).unwrap_err(),
            DecodeError::Truncated
        );
    }
}

/// The fuzz target starts from these: a seed that no longer decodes teaches it nothing.
#[test]
fn the_fuzz_targets_seed_states_are_states() {
    let seeds = concat!(env!("CARGO_MANIFEST_DIR"), "/fuzz/seeds/state_merge");
    let mut types = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(seeds).expect("the seed corpus") {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if name.starts_with("script_") {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        let state = KeyState::decode(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(encoded(&state), bytes, "{name}");
        types.insert(state.data().map_or("none", Data::type_name));
    }
    assert_eq!(
        types.into_iter().collect::<Vec<_>>(),
        ["first", "hll", "lww", "max", "min", "none", "sum", "union"]
    );
}

#[test]
fn the_term_order_is_total_where_erlangs_leaves_two_maps_undecided() {
    let all = values();
    for a in &all {
        assert_eq!(exact_order(a, a), Ordering::Equal);
        for b in &all {
            assert_eq!(
                exact_order(a, b),
                exact_order(b, a).reverse(),
                "{a:?} {b:?}"
            );
            for c in &all {
                if exact_order(a, b) != Ordering::Greater && exact_order(b, c) != Ordering::Greater
                {
                    assert_ne!(exact_order(a, c), Ordering::Greater, "{a:?} {b:?} {c:?}");
                }
            }
        }
    }
    // Two maps of one size: `Value::term_cmp` calls each less than the other.
    let (x, y) = (json("{\"x\":1}"), json("{\"y\":1}"));
    assert_eq!(x.term_cmp(&y), y.term_cmp(&x));
    assert_ne!(term_order(&x, &y), term_order(&y, &x));
    // Where Erlang's order decides, it is Erlang's.
    for (a, b) in [
        ("1", "\"a\""),
        ("1", "2.5"),
        ("true", "{}"),
        ("[1]", "[1,0]"),
    ] {
        assert_eq!(term_order(&json(a), &json(b)), Ordering::Less);
        assert_eq!(json(a).term_cmp(&json(b)), Ordering::Less);
    }
}
