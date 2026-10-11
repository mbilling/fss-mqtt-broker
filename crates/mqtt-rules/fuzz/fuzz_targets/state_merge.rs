#![no_main]
//! Fuzz target: the replicated form of a rule's state, and its merge (ADR 0085 D4).
//!
//! A peer sends a key's state as bytes, and three things must hold whatever the bytes
//! and whatever the order they arrive in:
//!
//! * **The decoder is total.** Any input is a state or an error, never a panic, and a
//!   state it accepts is canonical: encoding it gives the input back, within the bound
//!   on a key's replicated form.
//! * **The merge is a join.** Commutative, associative and idempotent over the states
//!   the input produced and the states it smuggled in.
//! * **Replicas converge.** The input is also read as a script for three nodes whose
//!   wall clocks disagree: local writes of every type, deletes, TTLs, a state handed
//!   from one node to another (or twice), decoded states injected as if a peer had
//!   sent them, tombstones forgotten. Once every node has heard every other, all three
//!   hold the same bytes for every key and read the same value.

use libfuzzer_sys::fuzz_target;
use mqtt_rules::crdt::{
    stable_hash, KeyState, Replica, Write, MAX_ENCODED_BYTES, MAX_FORWARD_SKEW_MS,
};
use mqtt_rules::Value;

const KEYS: [&[u8]; 3] = [b"a", b"b", b"c"];
const T0: u64 = 1_800_000_000_000;

struct Script<'a>(&'a [u8]);

impl<'a> Script<'a> {
    fn byte(&mut self) -> Option<u8> {
        let (first, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(*first)
    }

    fn take(&mut self, n: usize) -> &'a [u8] {
        let (head, rest) = self.0.split_at(n.min(self.0.len()));
        self.0 = rest;
        head
    }

    /// A value: a small integer, a float, text or bytes, or whatever JSON the next
    /// bytes are.
    fn value(&mut self) -> Value {
        let pick = self.byte().unwrap_or(0);
        match pick % 6 {
            0 => Value::Int(i64::from(pick / 6) - 20),
            1 => Value::Float(f64::from(pick / 6) / 4.0 - 2.0),
            2 => Value::Int(i64::MAX - i64::from(pick / 6)),
            3 => {
                let len = usize::from(pick / 6 % 12);
                Value::Bin(bytes::Bytes::copy_from_slice(self.take(len)))
            }
            4 => {
                let len = usize::from(pick / 6);
                mqtt_rules::json_decode(self.take(len)).unwrap_or(Value::Null)
            }
            _ => Value::Bool(pick / 6 % 2 == 0),
        }
    }
}

/// A state the decoder accepted: canonical, bounded, and its own join.
fn check_decoded(state: &KeyState, bytes: &[u8]) {
    assert!(bytes.len() <= MAX_ENCODED_BYTES);
    assert_eq!(state.encoded(), bytes, "decoding is not canonical");
    let mut twice = state.clone();
    twice.merge(state);
    assert_eq!(twice.encoded(), bytes, "merging a state with itself changed it");
}

fn merged(a: &KeyState, b: &KeyState) -> KeyState {
    let mut out = a.clone();
    out.merge(b);
    assert!(out.encoded().len() <= MAX_ENCODED_BYTES);
    out
}

fn check_join(a: &KeyState, b: &KeyState, c: &KeyState) {
    let ab = merged(a, b);
    assert_eq!(ab.encoded(), merged(b, a).encoded(), "not commutative");
    assert_eq!(merged(&ab, b).encoded(), ab.encoded(), "not idempotent");
    assert_eq!(
        merged(&ab, c).encoded(),
        merged(a, &merged(b, c)).encoded(),
        "not associative"
    );
}

fuzz_target!(|data: &[u8]| {
    if let Ok(state) = KeyState::decode(data) {
        check_decoded(&state, data);
    }

    let mut script = Script(data);
    let mut replicas = [Replica::new("n1"), Replica::new("n2"), Replica::new("n3")];
    // Each node's wall clock: a shared time, less what the node is behind by.
    let mut wall = T0;
    let mut behind = [0u64; 3];
    let mut injected: Vec<KeyState> = Vec::new();
    while let Some(op) = script.byte() {
        let node = usize::from(op >> 6) % 3;
        let key = KEYS[usize::from(op >> 4 & 3) % 3];
        let now = wall - behind[node];
        let detail = script.byte().unwrap_or(0);
        let ttl = [None, None, Some(1), Some(60)][usize::from(detail & 3)];
        let other = usize::from(detail >> 2) % 3;
        let value;
        let write = match op & 0x0f {
            0 | 1 => {
                value = script.value();
                Some(Write::Set(&value))
            }
            2 => {
                value = script.value();
                Some(Write::SetOnce(&value))
            }
            3 | 4 => {
                value = script.value();
                Some(Write::Extreme {
                    value: &value,
                    largest: op & 0x0f == 3,
                })
            }
            5 => {
                let by = [1, -1, 7, i64::MAX, i64::MIN, -40][usize::from(detail >> 2) % 6];
                Some(Write::Incr(by))
            }
            6 => {
                value = script.value();
                Some(Write::Add(&value))
            }
            7 => Some(Write::HllAdd(stable_hash(&[detail]))),
            8 => Some(Write::Delete),
            // One node's state of the key reaches another, once or twice. A node
            // whose clock is too far behind the sender's refuses it.
            9 | 10 => {
                if let Some(state) = replicas[node].state(key).cloned() {
                    for _ in 0..=(op & 1) {
                        let _ = replicas[other].receive(key, &state, wall - behind[other]);
                    }
                }
                None
            }
            // Time passes: nothing, a millisecond, a second, past the skew bound.
            11 => {
                wall += [0, 1, 1_000, 61_000][usize::from(detail >> 2) % 4];
                None
            }
            // A wall clock steps back, or is set right again.
            12 => {
                behind[node] = [0, 3, 900, 59_000][usize::from(detail >> 2) % 4];
                None
            }
            // Bytes from a peer: whatever decodes is merged, on one node.
            13 => {
                let len = usize::from(detail) * 4;
                let bytes = script.take(len);
                if let Ok(state) = KeyState::decode(bytes) {
                    check_decoded(&state, bytes);
                    let _ = replicas[node].receive(key, &state, now);
                    injected.push(state);
                }
                None
            }
            // What a delete left is forgotten after a window.
            _ => {
                replicas[node].forget(now, [0, 1_000, 3_600_000][usize::from(detail >> 2) % 3]);
                None
            }
        };
        if let Some(write) = write {
            // A refusal (another type, a full set, a counter at 64 bits) is an answer.
            let _ = replicas[node].write(key, &write, ttl, Some(600), now);
        }
    }

    // The join's laws, over what the nodes hold now and what was injected.
    for key in KEYS {
        let held: Vec<KeyState> = replicas
            .iter()
            .filter_map(|replica| replica.state(key).cloned())
            .chain(injected.iter().take(2).cloned())
            .collect();
        for a in &held {
            for b in &held {
                for c in &held {
                    check_join(a, b, c);
                }
            }
        }
    }

    // The partition heals: every clock is right again, and within the bound of every
    // stamp any node accepted. Twice round, so that what the last heard reaches the
    // first.
    let wall = wall + 2 * MAX_FORWARD_SKEW_MS;
    for _ in 0..2 {
        for from in 0..3 {
            for to in 0..3 {
                if from == to {
                    continue;
                }
                let states: Vec<(Vec<u8>, KeyState)> = replicas[from]
                    .states()
                    .map(|(key, state)| (key.to_vec(), state.clone()))
                    .collect();
                for (key, state) in states {
                    replicas[to]
                        .receive(&key, &state, wall)
                        .expect("a healed node accepts every stamp");
                }
            }
        }
    }
    let view = |replica: &Replica| -> Vec<(Vec<u8>, Vec<u8>, Option<String>)> {
        replica
            .states()
            .map(|(key, state)| {
                let bytes = state.encoded();
                assert!(bytes.len() <= MAX_ENCODED_BYTES);
                let back = KeyState::decode(&bytes).expect("a replica's state decodes");
                assert_eq!(back.encoded(), bytes);
                let read = replica.read(key, wall).and_then(|v| v.to_json().ok());
                (key.to_vec(), bytes, read)
            })
            .collect()
    };
    let first = view(&replicas[0]);
    assert_eq!(first, view(&replicas[1]), "replicas diverged");
    assert_eq!(first, view(&replicas[2]), "replicas diverged");
});
