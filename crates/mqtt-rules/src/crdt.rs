//! State that merges ([ADR 0085](../../../docs/adr/0085-rule-state-store.md) D4, D6):
//! what a key of a replicated namespace holds, how two copies of it become one, the
//! clock its writes are stamped with, and the bytes it travels as.
//!
//! Nothing here sends or receives anything, and nothing here holds broker state: these
//! are the data types the broker's store keeps a key in, the merge that a later task
//! runs when a peer's copy arrives, and the form that copy has on the wire.
//!
//! # A key's state, and its merge
//!
//! A [`KeyState`] is the latest delete the key has seen, what it holds since, and when
//! that expires. [`KeyState::merge`] is a join: commutative, associative and
//! idempotent over every state [`KeyState::decode`] accepts, so replicas that have
//! seen the same states hold the same bytes whatever order, grouping or repetition
//! they arrived in.
//!
//! * **A delete** is a stamp. A state that has seen a later delete than another's
//!   replaces what the other holds, with one exception.
//! * **`lww`** is one stamped write, and the exception: it survives every delete
//!   stamped before it, whoever had seen what, and the later of two writes wins. So a
//!   delete covers exactly the `lww` writes stamped before it.
//! * **Every other type** (`first`, `max`, `min`, `sum`, `union`, `hll`) holds what was
//!   written by nodes that had seen the state's delete. A delete therefore covers
//!   every write stamped before it and also the writes of nodes that had not seen it
//!   yet; a write made after seeing it is never covered. (No state of bounded size can
//!   do better for these types: the earliest of the writes a later delete leaves is
//!   not known until that delete is.) Two such states of one delete join by type: the
//!   earlier write, the larger or smaller number, each node's own counts, the union,
//!   the register-wise maximum.
//! * **Two types on one key** (two nodes created it at once with different functions):
//!   a surviving `lww` write wins, then the type later in [`Data`]'s order.
//! * **Expiry** travels with an `lww` write; for the other types it is the later of
//!   the two.
//!
//! # The clock
//!
//! [`Hlc`] is a hybrid logical clock: 48 bits of milliseconds and a 16-bit counter,
//! never behind the wall clock, never going back when the wall clock does, and moved
//! past every stamp the node accepts. A [`Stamp`] adds a tag of the node, which only
//! breaks ties. A stamp more than [`MAX_FORWARD_SKEW_MS`] ahead of this node's wall
//! clock is refused ([`Hlc::observe`]) instead of dragging this node's clock with it.
//!
//! # The replicated form
//!
//! [`KeyState::encode`] writes one key's whole state: at most [`MAX_ENCODED_BYTES`],
//! with values as ADR 0086 D3 carries them (a binary as its bytes, anything else as
//! JSON text). [`KeyState::decode`] is total: any bytes are a state or an error, it
//! allocates no more than its input justifies, and what it accepts is canonical
//! (encoding it again gives the same bytes).

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use bytes::Bytes;
use num_bigint::BigInt;

use crate::state::{term_key, MAX_ELEMENT_BYTES, MAX_SET_ELEMENTS, MAX_VALUE_BYTES};
use crate::value::Value;

/// How far ahead of this node's wall clock a stamp may be, in milliseconds. A state
/// that carries a later one is refused until the wall clock has caught up, so one
/// node with a wrong clock cannot move every other node's.
pub const MAX_FORWARD_SKEW_MS: u64 = 60_000;

/// The longest node id a counter's row names, in bytes.
pub const MAX_NODE_ID_BYTES: usize = 255;

/// The most node ids one counter keeps counts for.
pub const MAX_COUNTER_NODES: usize = 256;

/// A sketch's precision: 2^14 registers, a standard error of about 0.8%.
pub const HLL_PRECISION: u32 = 14;

/// A sketch's registers.
pub const HLL_REGISTERS: usize = 1 << HLL_PRECISION;

/// A dense sketch: six bits a register, 12 KiB.
pub const HLL_DENSE_BYTES: usize = HLL_REGISTERS * 6 / 8;

/// The most registers a sparse sketch lists (four bytes each in memory, so 12 KiB)
/// before it becomes a dense one.
pub const HLL_SPARSE_MAX: usize = HLL_DENSE_BYTES / 4;

/// The largest value a register holds: one more than the 50 bits a hash has left
/// after the register's index.
const HLL_MAX_RANK: u8 = 51;

/// The values a register can hold: 0 to [`HLL_MAX_RANK`].
const HLL_RANKS: usize = 52;

/// The version byte of the replicated form.
const VERSION: u8 = 1;

/// The most bytes a value takes in the replicated form: a tag, a length, the bytes.
const fn value_max(bytes: usize) -> usize {
    1 + 2 + bytes
}

/// The most bytes one key's state encodes to: the header and the largest payload, a
/// full set of the largest elements.
pub const MAX_ENCODED_BYTES: usize =
    1 + 16 + 10 + 1 + 2 + MAX_SET_ELEMENTS * value_max(MAX_ELEMENT_BYTES);

// -- the hash ---------------------------------------------------------------------------

/// The key of [`stable_hash`]: fixed, so that every node, architecture and release
/// hashes a value to the same 64 bits.
const HASH_KEY: (u64, u64) = (
    u64::from_le_bytes(*b"mqttd.kv"),
    u64::from_le_bytes(*b"_hll.v01"),
);

/// The hash a sketch (`kv_hll_add`) and a node's tag are made from: SipHash-2-4 under
/// a fixed key, over the bytes given. It must never change: sketches made by two
/// releases are merged register by register.
#[must_use]
pub fn stable_hash(bytes: &[u8]) -> u64 {
    siphash24(HASH_KEY.0, HASH_KEY.1, bytes)
}

/// SipHash-2-4 (Aumasson and Bernstein), as its reference implementation computes it.
fn siphash24(k0: u64, k1: u64, data: &[u8]) -> u64 {
    fn round(v: &mut [u64; 4]) {
        v[0] = v[0].wrapping_add(v[1]);
        v[1] = v[1].rotate_left(13) ^ v[0];
        v[0] = v[0].rotate_left(32);
        v[2] = v[2].wrapping_add(v[3]);
        v[3] = v[3].rotate_left(16) ^ v[2];
        v[0] = v[0].wrapping_add(v[3]);
        v[3] = v[3].rotate_left(21) ^ v[0];
        v[2] = v[2].wrapping_add(v[1]);
        v[1] = v[1].rotate_left(17) ^ v[2];
        v[2] = v[2].rotate_left(32);
    }
    fn absorb(v: &mut [u64; 4], word: u64) {
        v[3] ^= word;
        round(v);
        round(v);
        v[0] ^= word;
    }
    let mut v = [
        k0 ^ 0x736f_6d65_7073_6575,
        k1 ^ 0x646f_7261_6e64_6f6d,
        k0 ^ 0x6c79_6765_6e65_7261,
        k1 ^ 0x7465_6462_7974_6573,
    ];
    let mut words = data.chunks_exact(8);
    for word in &mut words {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(word);
        absorb(&mut v, u64::from_le_bytes(bytes));
    }
    let rest = words.remainder();
    let mut last = [0u8; 8];
    last[..rest.len()].copy_from_slice(rest);
    last[7] = data.len().to_le_bytes()[0];
    absorb(&mut v, u64::from_le_bytes(last));
    v[2] ^= 0xff;
    for _ in 0..4 {
        round(&mut v);
    }
    v[0] ^ v[1] ^ v[2] ^ v[3]
}

// -- the clock --------------------------------------------------------------------------

/// The most milliseconds a stamp carries: 48 bits.
const MAX_MS: u64 = (1 << 48) - 1;

/// When a write or a delete happened, by its node's [`Hlc`]: milliseconds and a
/// counter, then a tag of the node. Ordered by time, the tag deciding between two
/// nodes' stamps of one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Stamp {
    /// Milliseconds since the Unix epoch in the upper 48 bits, the counter below.
    time: u64,
    /// The node's tag ([`Hlc::new`]).
    node: u64,
}

impl Stamp {
    /// Before every stamp a clock gives: no write, no delete.
    pub const ZERO: Self = Self { time: 0, node: 0 };

    /// A stamp from its parts. `ms` is cut to 48 bits.
    #[must_use]
    #[allow(clippy::cast_lossless)]
    pub const fn new(ms: u64, counter: u16, node: u64) -> Self {
        let ms = if ms > MAX_MS { MAX_MS } else { ms };
        Self {
            // A widening: `u64::from` is not `const`.
            time: (ms << 16) | counter as u64,
            node,
        }
    }

    /// Its physical part: milliseconds since the Unix epoch.
    #[must_use]
    pub const fn ms(self) -> u64 {
        self.time >> 16
    }

    /// Its logical part: what orders the stamps of one millisecond.
    #[must_use]
    pub fn counter(self) -> u16 {
        u16::try_from(self.time & 0xffff).unwrap_or(u16::MAX)
    }

    /// The tag of the node that gave it.
    #[must_use]
    pub const fn node(self) -> u64 {
        self.node
    }

    fn put(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.time.to_be_bytes());
        out.extend_from_slice(&self.node.to_be_bytes());
    }
}

/// A stamp too far ahead of this node's wall clock to accept ([`MAX_FORWARD_SKEW_MS`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "a stamp {ahead_ms} ms ahead of this node's clock (at most {MAX_FORWARD_SKEW_MS} is accepted)"
)]
pub struct Skew {
    /// How far ahead it is, in milliseconds.
    pub ahead_ms: u64,
}

/// One node's hybrid logical clock (D6). Shared by every task that writes: a stamp is
/// one compare-and-swap.
#[derive(Debug)]
pub struct Hlc {
    /// The time of the latest stamp given or accepted.
    last: AtomicU64,
    node: u64,
}

impl Hlc {
    /// The clock of the node `node_id`. Its stamps carry [`stable_hash`] of the id, to
    /// break ties; two ids with one tag would only make a tie fall to the values.
    #[must_use]
    pub fn new(node_id: &str) -> Self {
        Self {
            last: AtomicU64::new(0),
            node: stable_hash(node_id.as_bytes()),
        }
    }

    /// A stamp for a write happening now, `wall_ms` being the wall clock: later than
    /// every stamp this clock has given or accepted, and not before the wall clock.
    /// When the wall clock stands still or steps back, the counter carries on (and
    /// carries into the milliseconds after 65,536 stamps of one millisecond).
    pub fn now(&self, wall_ms: u64) -> Stamp {
        let wall = wall_ms.min(MAX_MS) << 16;
        let mut time = wall;
        // The closure always answers, so this cannot fail.
        let _ = self.last.fetch_update(Relaxed, Relaxed, |last| {
            time = wall.max(last.saturating_add(1));
            Some(time)
        });
        Stamp {
            time,
            node: self.node,
        }
    }

    /// Take note of a stamp that arrived from another node, so that what this node
    /// writes next is stamped after it. `Err`, and the clock is left alone, when the
    /// stamp is more than [`MAX_FORWARD_SKEW_MS`] ahead of the wall clock: the state
    /// that carries it must not be merged yet.
    pub fn observe(&self, seen: Stamp, wall_ms: u64) -> Result<(), Skew> {
        let ahead_ms = seen.ms().saturating_sub(wall_ms.min(MAX_MS));
        if ahead_ms > MAX_FORWARD_SKEW_MS {
            return Err(Skew { ahead_ms });
        }
        self.last.fetch_max(seen.time, Relaxed);
        Ok(())
    }

    /// The clock's physical part, in milliseconds: the wall clock, or the latest stamp
    /// when that is ahead of it. Never goes back. What expiries are judged by.
    pub fn physical_ms(&self, wall_ms: u64) -> u64 {
        wall_ms.min(MAX_MS).max(self.last.load(Relaxed) >> 16)
    }
}

// -- values -----------------------------------------------------------------------------

/// The order sets are kept in and numbers are compared by: Erlang's term order
/// ([`Value::term_cmp`]), with two maps of one size ordered by their identity
/// ([`term_key`]) where that order does not decide. Values that are equal in term
/// order but not exactly (`1` and `1.0`) compare equal here too.
#[must_use]
pub fn term_order(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Array(x), Value::Array(y)) => {
            for (p, q) in x.iter().zip(y.iter()) {
                match term_order(p, q) {
                    Ordering::Equal => {}
                    decided => return decided,
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Map(x), Value::Map(y)) => x
            .len()
            .cmp(&y.len())
            .then_with(|| term_key(a).cmp(&term_key(b))),
        _ => a.term_cmp(b),
    }
}

/// A value as the replicated form writes it: whether it is a binary, and its bytes.
fn written(v: &Value) -> (bool, std::borrow::Cow<'_, [u8]>) {
    match v.as_bytes() {
        Some(bytes) if v.is_binary() => (true, std::borrow::Cow::Borrowed(bytes)),
        _ => (
            false,
            std::borrow::Cow::Owned(v.to_json().unwrap_or_default().into_bytes()),
        ),
    }
}

/// A total order on values: [`term_order`], then identity, then the bytes they are
/// written as. Only values that are written the same compare equal, so choosing by it
/// is choosing the same value on every node.
fn exact_order(a: &Value, b: &Value) -> Ordering {
    term_order(a, b)
        .then_with(|| term_key(a).cmp(&term_key(b)))
        .then_with(|| written(a).cmp(&written(b)))
}

/// A value that cannot be kept in a replicated key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the value cannot be replicated: {0}")]
pub struct NotStorable(&'static str);

/// `v` as every replica will hold it: what reading its replicated form back gives.
/// The form is ADR 0086 D3's (a binary as its bytes, anything else as JSON text), so
/// three things do not survive it and are settled here, on the node that writes:
/// `-0.0` becomes `0.0`, a binary that is not UTF-8 *inside* an array or a map becomes
/// text with U+FFFD in place of each broken sequence (on its own it keeps its bytes),
/// and `undefined` inside one becomes the text `undefined`. Everything else, an
/// integer of any size and the order of a map's keys included, comes back as it was.
pub fn normal(v: &Value) -> Result<Value, NotStorable> {
    match v {
        Value::Undefined => Err(NotStorable("undefined is not a value")),
        Value::Str(_) | Value::Int(_) | Value::Bool(_) | Value::Null => Ok(v.clone()),
        Value::Bin(bytes) => Ok(Value::from_bytes(&Bytes::copy_from_slice(bytes))),
        _ => {
            let mut out = Vec::new();
            put_value(v, &mut out);
            let mut reader = Reader { rest: &out };
            reader
                .value(MAX_VALUE_BYTES)
                .map_err(|_| NotStorable("it does not read back from its JSON text"))
        }
    }
}

fn put_varint(mut n: u128, out: &mut Vec<u8>) {
    loop {
        let low = n.to_le_bytes()[0] & 0x7f;
        n >>= 7;
        if n == 0 {
            out.push(low);
            return;
        }
        out.push(low | 0x80);
    }
}

fn put_len(n: usize, out: &mut Vec<u8>) {
    put_varint(u128::try_from(n).unwrap_or(u128::MAX), out);
}

fn put_value(v: &Value, out: &mut Vec<u8>) {
    let (binary, bytes) = written(v);
    out.push(if binary { b'b' } else { b'j' });
    put_len(bytes.len(), out);
    out.extend_from_slice(&bytes);
}

/// Bytes that are not one key's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The bytes end before the state does.
    #[error("the state is cut short")]
    Truncated,
    /// More bytes than any state encodes to.
    #[error("the state is larger than a key's may be")]
    TooLarge,
    /// A version this release does not read.
    #[error("unknown state version {0}")]
    Version(u8),
    /// Not a state, or not written the one way a state is.
    #[error("malformed state: {0}")]
    Malformed(&'static str),
}

struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.rest.len() < n {
            return Err(DecodeError::Truncated);
        }
        let (head, rest) = self.rest.split_at(n);
        self.rest = rest;
        Ok(head)
    }

    fn byte(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(bytes))
    }

    fn stamp(&mut self) -> Result<Stamp, DecodeError> {
        Ok(Stamp {
            time: self.u64()?,
            node: self.u64()?,
        })
    }

    /// A LEB128 number of at most 128 bits, written in the fewest bytes.
    fn varint(&mut self) -> Result<u128, DecodeError> {
        let mut n = 0u128;
        for at in 0..19u32 {
            let byte = self.byte()?;
            let bits = u128::from(byte & 0x7f);
            if at == 18 && bits > 3 {
                break;
            }
            n |= bits << (7 * at);
            if byte & 0x80 == 0 {
                if byte == 0 && at > 0 {
                    return Err(DecodeError::Malformed("a number written long"));
                }
                return Ok(n);
            }
        }
        Err(DecodeError::Malformed("a number past 128 bits"))
    }

    /// A count or a length of at most `max`.
    fn len(&mut self, max: usize, what: &'static str) -> Result<usize, DecodeError> {
        usize::try_from(self.varint()?)
            .ok()
            .filter(|n| *n <= max)
            .ok_or(DecodeError::Malformed(what))
    }

    fn u64_varint(&mut self) -> Result<u64, DecodeError> {
        u64::try_from(self.varint()?).map_err(|_| DecodeError::Malformed("a time past 64 bits"))
    }

    /// One value of at most `max` bytes as written, in the one form it is written in.
    fn value(&mut self, max: usize) -> Result<Value, DecodeError> {
        let tag = self.byte()?;
        let len = self.len(max, "a value past its bound")?;
        let bytes = self.take(len)?;
        match tag {
            b'b' => Ok(Value::from_bytes(&Bytes::copy_from_slice(bytes))),
            b'j' => {
                let value = crate::value::json_decode(bytes)
                    .map_err(|_| DecodeError::Malformed("a value that is not JSON"))?;
                // Text travels as a binary, and the writer has one text per value.
                if value.is_binary()
                    || value.is_undefined()
                    || value.to_json().ok().as_deref().map(str::as_bytes) != Some(bytes)
                {
                    return Err(DecodeError::Malformed(
                        "a value not written as the rule engine writes it",
                    ));
                }
                Ok(value)
            }
            _ => Err(DecodeError::Malformed("an unknown value tag")),
        }
    }
}

// -- a counter --------------------------------------------------------------------------

/// One node's own increments and decrements of a counter.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    node: Box<str>,
    up: u128,
    down: u128,
}

/// Why a counter did not take an increment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountsError {
    /// The count would pass 64 bits.
    Overflow,
    /// The counter already has counts of [`MAX_COUNTER_NODES`] nodes, and this node is
    /// not one of them.
    TooManyNodes,
    /// The node's id is longer than [`MAX_NODE_ID_BYTES`].
    NodeId,
}

/// A counter's rows, by node id, ascending. One node's counts are held in place: a
/// counter that only this node counts is the usual one, and on the publish path it is
/// then one cache line less to fetch than a list would be.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Rows {
    /// Exactly one node's.
    One(Row),
    /// None, or two and more.
    Many(Vec<Row>),
}

/// A counter (`kv_incr`, type `sum`): for each node that ever counted, how much it
/// added and how much it took away. A node only ever raises its own two numbers, so
/// two copies merge by taking the larger of each, and the count is the sum of what
/// was added less the sum of what was taken away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Counts {
    rows: Rows,
}

impl Default for Counts {
    fn default() -> Self {
        Self {
            rows: Rows::Many(Vec::new()),
        }
    }
}

impl Counts {
    fn from_rows(mut rows: Vec<Row>) -> Self {
        let rows = match rows.pop() {
            Some(only) if rows.is_empty() => Rows::One(only),
            Some(last) => {
                rows.push(last);
                Rows::Many(rows)
            }
            None => Rows::Many(rows),
        };
        Self { rows }
    }

    fn rows(&self) -> &[Row] {
        match &self.rows {
            Rows::One(row) => std::slice::from_ref(row),
            Rows::Many(rows) => rows,
        }
    }

    fn rows_mut(&mut self) -> &mut [Row] {
        match &mut self.rows {
            Rows::One(row) => std::slice::from_mut(row),
            Rows::Many(rows) => rows,
        }
    }

    fn insert(&mut self, at: usize, row: Row) {
        let mut rows = match std::mem::replace(&mut self.rows, Rows::Many(Vec::new())) {
            Rows::One(only) => vec![only],
            Rows::Many(rows) => rows,
        };
        rows.insert(at, row);
        *self = Self::from_rows(rows);
    }

    /// The count, when each side's sum fits 128 bits (it does unless hundreds of
    /// nodes each counted past them).
    fn total_i128(&self) -> Option<i128> {
        let mut up = 0u128;
        let mut down = 0u128;
        for row in self.rows() {
            up = up.checked_add(row.up)?;
            down = down.checked_add(row.down)?;
        }
        if up >= down {
            i128::try_from(up - down).ok()
        } else {
            i128::try_from(down - up).ok().map(|n| -n)
        }
    }

    /// The count, when it fits 64 bits.
    #[must_use]
    pub fn total_i64(&self) -> Option<i64> {
        self.total_i128().and_then(|n| i64::try_from(n).ok())
    }

    /// The count, of any size: merged counts of several nodes can pass 64 bits though
    /// no node's own did.
    #[must_use]
    pub fn total(&self) -> Value {
        if let Some(n) = self.total_i64() {
            return Value::Int(n);
        }
        let mut total = BigInt::from(0);
        for row in self.rows() {
            total += BigInt::from(row.up);
            total -= BigInt::from(row.down);
        }
        crate::num::from_big(total).unwrap_or(Value::Int(i64::MAX))
    }

    /// How many nodes it has counts of.
    #[must_use]
    pub fn nodes(&self) -> usize {
        self.rows().len()
    }

    /// The bytes of the node ids it has counts of: what a store accounts a counter
    /// of several nodes as, beyond its numbers.
    #[must_use]
    pub fn node_bytes(&self) -> usize {
        self.rows().iter().map(|row| row.node.len()).sum()
    }

    /// Add `by` on behalf of `node` and answer the count after, which must fit 64
    /// bits. Nothing changes when it is refused.
    pub fn add(&mut self, node: &str, by: i64) -> Result<i64, CountsError> {
        // The usual counter: this node's counts and no other's, each within 63 bits.
        if let Rows::One(row) = &mut self.rows {
            if *row.node == *node {
                if let (Ok(up), Ok(down)) = (i64::try_from(row.up), i64::try_from(row.down)) {
                    let after = (up - down).checked_add(by).ok_or(CountsError::Overflow)?;
                    let step = u128::from(by.unsigned_abs());
                    if by >= 0 {
                        row.up += step;
                    } else {
                        row.down += step;
                    }
                    return Ok(after);
                }
            }
        }
        let after = self
            .total_i128()
            .and_then(|total| total.checked_add(i128::from(by)))
            .and_then(|after| i64::try_from(after).ok())
            .ok_or(CountsError::Overflow)?;
        let at = match self.rows().binary_search_by(|row| (*row.node).cmp(node)) {
            Ok(at) => at,
            Err(_) if node.len() > MAX_NODE_ID_BYTES => return Err(CountsError::NodeId),
            Err(_) if self.nodes() >= MAX_COUNTER_NODES => return Err(CountsError::TooManyNodes),
            Err(at) => {
                let row = Row {
                    node: Box::from(node),
                    up: 0,
                    down: 0,
                };
                self.insert(at, row);
                at
            }
        };
        let row = &mut self.rows_mut()[at];
        let side = if by >= 0 { &mut row.up } else { &mut row.down };
        // A node would have to count 2^64 times by 2^63 to get here.
        *side = side
            .checked_add(u128::from(by.unsigned_abs()))
            .ok_or(CountsError::Overflow)?;
        Ok(after)
    }

    /// The larger of each node's two numbers; past [`MAX_COUNTER_NODES`] nodes, those
    /// first in order of id.
    fn merge(&mut self, other: &Counts) {
        let mut rows = match std::mem::replace(&mut self.rows, Rows::Many(Vec::new())) {
            Rows::One(only) => vec![only],
            Rows::Many(rows) => rows,
        };
        for theirs in other.rows() {
            match rows.binary_search_by(|row| row.node.cmp(&theirs.node)) {
                Ok(at) => {
                    let row = &mut rows[at];
                    row.up = row.up.max(theirs.up);
                    row.down = row.down.max(theirs.down);
                }
                Err(at) => rows.insert(at, theirs.clone()),
            }
        }
        rows.truncate(MAX_COUNTER_NODES);
        *self = Self::from_rows(rows);
    }

    fn put(&self, out: &mut Vec<u8>) {
        put_len(self.nodes(), out);
        for row in self.rows() {
            out.push(u8::try_from(row.node.len()).unwrap_or(u8::MAX));
            out.extend_from_slice(row.node.as_bytes());
            put_varint(row.up, out);
            put_varint(row.down, out);
        }
    }

    fn read(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let count = reader.len(MAX_COUNTER_NODES, "a counter of too many nodes")?;
        // A row is at least three bytes: no count is believed past what is there.
        let mut rows: Vec<Row> = Vec::with_capacity(count.min(reader.rest.len() / 3));
        for _ in 0..count {
            let len = usize::from(reader.byte()?);
            let node = std::str::from_utf8(reader.take(len)?)
                .map_err(|_| DecodeError::Malformed("a node id that is not UTF-8"))?;
            if rows.last().is_some_and(|last| *last.node >= *node) {
                return Err(DecodeError::Malformed("a counter's nodes out of order"));
            }
            rows.push(Row {
                node: Box::from(node),
                up: reader.varint()?,
                down: reader.varint()?,
            });
        }
        Ok(Self::from_rows(rows))
    }
}

// -- a set ------------------------------------------------------------------------------

/// A set that already holds [`MAX_SET_ELEMENTS`] elements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetFull;

/// A set (`kv_add`, type `union`): its elements in [`term_order`], each with its
/// identity ([`term_key`]), which is what makes two elements one and what orders
/// elements that the term order calls equal (`1` and `1.0`).
#[derive(Debug, Clone, Default)]
pub struct Elements {
    items: Vec<(Box<[u8]>, Value)>,
}

impl Elements {
    /// How many elements it holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether it holds none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// The elements, in order.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.items.iter().map(|(_, value)| value)
    }

    fn place(&self, identity: &[u8], element: &Value) -> Result<usize, usize> {
        let at = self.items.partition_point(|(id, held)| {
            term_order(held, element).then_with(|| (**id).cmp(identity)) == Ordering::Less
        });
        match self.items.get(at) {
            Some((id, _)) if **id == *identity => Ok(at),
            _ => Err(at),
        }
    }

    /// Add `element`, whose identity is `identity`, unless the set holds it; `keep`
    /// makes the copy the set keeps. Whether it was new. Nothing changes when the set
    /// is full.
    pub fn insert(
        &mut self,
        identity: &[u8],
        element: &Value,
        keep: impl FnOnce(&Value) -> Value,
    ) -> Result<bool, SetFull> {
        match self.place(identity, element) {
            Ok(_) => Ok(false),
            Err(_) if self.items.len() >= MAX_SET_ELEMENTS => Err(SetFull),
            Err(at) => {
                self.items.insert(at, (Box::from(identity), keep(element)));
                Ok(true)
            }
        }
    }

    /// The union; past [`MAX_SET_ELEMENTS`], the elements first in order. Of two
    /// elements with one identity written differently (a map's keys in another
    /// order), the one [`exact_order`] puts first.
    fn merge(&mut self, other: &Elements) {
        let mine = std::mem::take(&mut self.items);
        let mut merged = Vec::with_capacity(mine.len().max(other.items.len()));
        let mut mine = mine.into_iter().peekable();
        let mut theirs = other.items.iter().peekable();
        while merged.len() < MAX_SET_ELEMENTS {
            let order = match (mine.peek(), theirs.peek()) {
                (Some(a), Some(b)) => term_order(&a.1, &b.1).then_with(|| a.0.cmp(&b.0)),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => break,
            };
            match order {
                Ordering::Less => merged.extend(mine.next()),
                Ordering::Greater => merged.extend(theirs.next().cloned()),
                Ordering::Equal => {
                    if let (Some(a), Some(b)) = (mine.next(), theirs.next()) {
                        merged.push(if exact_order(&a.1, &b.1) == Ordering::Greater {
                            b.clone()
                        } else {
                            a
                        });
                    }
                }
            }
        }
        self.items = merged;
    }

    fn read(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let count = reader.len(MAX_SET_ELEMENTS, "a set of too many elements")?;
        // An element is at least two bytes.
        let mut items: Vec<(Box<[u8]>, Value)> =
            Vec::with_capacity(count.min(reader.rest.len() / 2));
        for _ in 0..count {
            let value = reader.value(MAX_ELEMENT_BYTES)?;
            let identity = term_key(&value).into_boxed_slice();
            if items.last().is_some_and(|(id, last)| {
                term_order(last, &value).then_with(|| id.cmp(&identity)) != Ordering::Less
            }) {
                return Err(DecodeError::Malformed("a set's elements out of order"));
            }
            items.push((identity, value));
        }
        Ok(Self { items })
    }
}

// -- a sketch ---------------------------------------------------------------------------

/// A sketch's registers: listed while few are set, an array once listing them would
/// take as much.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Registers {
    /// The registers that are set, ascending: the index in the upper 24 bits, its
    /// value in the lowest 8. At most [`HLL_SPARSE_MAX`].
    Sparse(Vec<u32>),
    /// Every register, six bits each, the first in the lowest bits of the first byte:
    /// [`HLL_DENSE_BYTES`].
    Dense(Box<[u8]>),
}

/// A `HyperLogLog` sketch (`kv_hll_add`, type `hll`): an estimate of how many distinct
/// values were added, from [`HLL_REGISTERS`] six-bit registers. The standard error is
/// `1.04 / sqrt(16384)`, about 0.8%. Two sketches merge register by register, the
/// larger value kept, which is the sketch of everything either saw.
///
/// A value is added by its [`stable_hash`]: the upper [`HLL_PRECISION`] bits choose
/// the register, and the register keeps the largest count of leading zeros (plus one)
/// seen in the other 50.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hll {
    registers: Registers,
    /// How many registers hold each value: what the estimate is computed from, kept
    /// as registers change so that an estimate does not read them all.
    histogram: [u16; HLL_RANKS],
}

impl Default for Hll {
    fn default() -> Self {
        let mut histogram = [0; HLL_RANKS];
        histogram[0] = u16::try_from(HLL_REGISTERS).unwrap_or(u16::MAX);
        Self {
            registers: Registers::Sparse(Vec::new()),
            histogram,
        }
    }
}

fn dense_get(dense: &[u8], index: usize) -> u8 {
    let (byte, shift) = (index * 6 / 8, index * 6 % 8);
    let low = dense.get(byte).copied().unwrap_or(0);
    let high = dense.get(byte + 1).copied().unwrap_or(0);
    (u16::from_le_bytes([low, high]) >> shift).to_le_bytes()[0] & 0x3f
}

fn dense_set(dense: &mut [u8], index: usize, rank: u8) {
    let (byte, shift) = (index * 6 / 8, index * 6 % 8);
    let low = dense.get(byte).copied().unwrap_or(0);
    let high = dense.get(byte + 1).copied().unwrap_or(0);
    let word = (u16::from_le_bytes([low, high]) & !(0x3f << shift)) | (u16::from(rank) << shift);
    let [low, high] = word.to_le_bytes();
    if let Some(at) = dense.get_mut(byte) {
        *at = low;
    }
    if let Some(at) = dense.get_mut(byte + 1) {
        *at = high;
    }
}

impl Hll {
    /// Add the value whose [`stable_hash`] is `hash`. Whether a register rose (the
    /// estimate can only have changed then).
    pub fn add(&mut self, hash: u64) -> bool {
        let index = usize::try_from(hash >> (64 - HLL_PRECISION)).unwrap_or(0);
        // The 50 bits below the index, with a one after them: at most 50 zeros lead.
        let rest = (hash << HLL_PRECISION) | (1 << (HLL_PRECISION - 1));
        let rank = u8::try_from(rest.leading_zeros() + 1).unwrap_or(HLL_MAX_RANK);
        self.raise(index, rank)
    }

    /// Raise register `index` to `rank` when it holds less.
    fn raise(&mut self, index: usize, rank: u8) -> bool {
        let held = match &mut self.registers {
            Registers::Dense(dense) => {
                let held = dense_get(dense, index);
                if held >= rank {
                    return false;
                }
                dense_set(dense, index, rank);
                held
            }
            Registers::Sparse(list) => {
                let entry = (u32::try_from(index).unwrap_or(0) << 8) | u32::from(rank);
                match list.binary_search_by_key(&(entry >> 8), |held| held >> 8) {
                    Ok(at) => {
                        let held = list[at].to_le_bytes()[0];
                        if held >= rank {
                            return false;
                        }
                        list[at] = entry;
                        held
                    }
                    Err(at) if list.len() < HLL_SPARSE_MAX => {
                        if list.len() == list.capacity() {
                            // Never room for more than the most it may list, so that
                            // the list never takes more than the array would.
                            let room = (list.capacity() * 2).clamp(8, HLL_SPARSE_MAX);
                            list.reserve_exact(room - list.len());
                        }
                        list.insert(at, entry);
                        0
                    }
                    Err(_) => {
                        let mut dense = vec![0u8; HLL_DENSE_BYTES].into_boxed_slice();
                        for held in list.iter() {
                            let at = usize::try_from(held >> 8).unwrap_or(0);
                            dense_set(&mut dense, at, held.to_le_bytes()[0]);
                        }
                        dense_set(&mut dense, index, rank);
                        self.registers = Registers::Dense(dense);
                        0
                    }
                }
            }
        };
        self.histogram[usize::from(held)] -= 1;
        self.histogram[usize::from(rank)] += 1;
        true
    }

    /// The bytes the registers take in memory: four for each one the list has room
    /// for, or [`HLL_DENSE_BYTES`]. Never more than that, and never less than before.
    #[must_use]
    pub fn bytes(&self) -> usize {
        match &self.registers {
            Registers::Sparse(list) => list.capacity() * 4,
            Registers::Dense(dense) => dense.len(),
        }
    }

    /// The estimate of how many distinct values were added: Ertl's improved estimator
    /// ("New cardinality estimation algorithms for `HyperLogLog` sketches", 2017,
    /// algorithm 6), which needs no correction table for small or large counts. Only
    /// additions, halvings and square roots of doubles, so every node computes the
    /// same number from the same registers.
    #[must_use]
    pub fn count(&self) -> u64 {
        fn sigma(mut x: f64) -> f64 {
            if x >= 1.0 {
                return f64::INFINITY;
            }
            let (mut y, mut z) = (1.0, x);
            loop {
                x *= x;
                let before = z;
                z += x * y;
                y += y;
                if before.to_bits() == z.to_bits() {
                    return z;
                }
            }
        }
        fn tau(mut x: f64) -> f64 {
            if x <= 0.0 || x >= 1.0 {
                return 0.0;
            }
            let (mut y, mut z) = (1.0, 1.0 - x);
            loop {
                x = x.sqrt();
                let before = z;
                y *= 0.5;
                z -= (1.0 - x) * (1.0 - x) * y;
                if before.to_bits() == z.to_bits() {
                    return z / 3.0;
                }
            }
        }
        // 1 / (2 ln 2).
        const ALPHA: f64 = 0.721_347_520_444_481_7;
        let m = f64::from(1u32 << HLL_PRECISION);
        let of = |rank: usize| f64::from(self.histogram[rank]);
        let top = usize::from(HLL_MAX_RANK);
        let mut z = m * tau(1.0 - of(top) / m);
        for rank in (1..top).rev() {
            z = 0.5 * (z + of(rank));
        }
        z += m * sigma(of(0) / m);
        let estimate = (ALPHA * m * m / z).round();
        // Finite and not negative: `z` is at least a positive term, or infinite.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let count = estimate as u64;
        count
    }

    /// The larger of each register.
    fn merge(&mut self, other: &Hll) {
        match &other.registers {
            Registers::Sparse(list) => {
                for held in list {
                    let at = usize::try_from(held >> 8).unwrap_or(0);
                    self.raise(at, held.to_le_bytes()[0]);
                }
            }
            Registers::Dense(dense) => {
                for at in 0..HLL_REGISTERS {
                    let rank = dense_get(dense, at);
                    if rank != 0 {
                        self.raise(at, rank);
                    }
                }
            }
        }
    }

    fn put(&self, out: &mut Vec<u8>) {
        match &self.registers {
            Registers::Sparse(list) => {
                out.push(0);
                put_len(list.len(), out);
                for held in list {
                    let [rank, low, high, _] = held.to_le_bytes();
                    out.extend_from_slice(&[high, low, rank]);
                }
            }
            Registers::Dense(dense) => {
                out.push(1);
                out.extend_from_slice(dense);
            }
        }
    }

    fn read(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let mut sketch = Self::default();
        match reader.byte()? {
            0 => {
                let count = reader.len(HLL_SPARSE_MAX, "a sparse sketch of too many registers")?;
                let mut list = Vec::with_capacity(count.min(reader.rest.len() / 3));
                for _ in 0..count {
                    let [high, low, rank] = *reader.take(3)? else {
                        return Err(DecodeError::Truncated);
                    };
                    let index = u32::from(u16::from_be_bytes([high, low]));
                    let entry = (index << 8) | u32::from(rank);
                    if usize::try_from(index).map_or(true, |at| at >= HLL_REGISTERS)
                        || !(1..=HLL_MAX_RANK).contains(&rank)
                        || list.last().is_some_and(|last: &u32| last >> 8 >= index)
                    {
                        return Err(DecodeError::Malformed("a sketch's registers"));
                    }
                    sketch.histogram[0] -= 1;
                    sketch.histogram[usize::from(rank)] += 1;
                    list.push(entry);
                }
                sketch.registers = Registers::Sparse(list);
            }
            1 => {
                let dense: Box<[u8]> = Box::from(reader.take(HLL_DENSE_BYTES)?);
                let mut set = 0usize;
                for at in 0..HLL_REGISTERS {
                    let rank = dense_get(&dense, at);
                    if rank > HLL_MAX_RANK {
                        return Err(DecodeError::Malformed("a sketch's registers"));
                    }
                    if rank != 0 {
                        set += 1;
                        sketch.histogram[0] -= 1;
                        sketch.histogram[usize::from(rank)] += 1;
                    }
                }
                // A sketch that fits the list is written as the list.
                if set <= HLL_SPARSE_MAX {
                    return Err(DecodeError::Malformed("a dense sketch of few registers"));
                }
                sketch.registers = Registers::Dense(dense);
            }
            _ => return Err(DecodeError::Malformed("an unknown sketch form")),
        }
        Ok(sketch)
    }
}

// -- a key's state ----------------------------------------------------------------------

/// What a key holds: its type is that of the operation that created it (D4). The
/// order of the variants after `Lww` is the order that decides between two types on
/// one key: the later wins.
#[derive(Debug, Clone)]
pub enum Data {
    /// `kv_set`, `kv_changed`, `kv_store_put`: the latest write.
    Lww {
        /// When it was written ([`Stamp::ZERO`] in a namespace that is not replicated).
        stamp: Stamp,
        /// The value.
        value: Value,
    },
    /// `kv_set_once`, `kv_held`: the earliest write.
    First {
        /// When it was written ([`Stamp::ZERO`] in a namespace that is not replicated).
        stamp: Stamp,
        /// The value.
        value: Value,
    },
    /// `kv_max`: the largest number offered.
    Max(Value),
    /// `kv_min`: the smallest number offered.
    Min(Value),
    /// `kv_incr`: a counter.
    Sum(Counts),
    /// `kv_add`: a set.
    Union(Elements),
    /// `kv_hll_add`: a sketch of distinct values.
    Hll(Box<Hll>),
}

impl Data {
    /// The type's name, as D4 and `wrong_type` name it.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Lww { .. } => "lww",
            Self::First { .. } => "first",
            Self::Max(_) => "max",
            Self::Min(_) => "min",
            Self::Sum(_) => "sum",
            Self::Union(_) => "union",
            Self::Hll(_) => "hll",
        }
    }

    /// The type's tag in the replicated form, which is also its rank.
    fn tag(&self) -> u8 {
        match self {
            Self::Lww { .. } => 1,
            Self::First { .. } => 2,
            Self::Max(_) => 3,
            Self::Min(_) => 4,
            Self::Sum(_) => 5,
            Self::Union(_) => 6,
            Self::Hll(_) => 7,
        }
    }

    /// What `kv_get` reads: the value, a counter's count, a set's elements in order,
    /// a sketch's estimate.
    #[must_use]
    pub fn read(&self) -> Value {
        match self {
            Self::Lww { value, .. }
            | Self::First { value, .. }
            | Self::Max(value)
            | Self::Min(value) => value.clone(),
            Self::Sum(counts) => counts.total(),
            Self::Union(elements) => Value::from(elements.iter().cloned().collect::<Vec<_>>()),
            Self::Hll(sketch) => Value::Int(i64::try_from(sketch.count()).unwrap_or(i64::MAX)),
        }
    }
}

/// An expiry, with "never" (`0`) as the latest of all.
fn never_last(expires_ms: u64) -> u64 {
    if expires_ms == 0 {
        u64::MAX
    } else {
        expires_ms
    }
}

/// The later of two expiries.
fn later(a: u64, b: u64) -> u64 {
    if a == 0 || b == 0 {
        0
    } else {
        a.max(b)
    }
}

/// Whether what a state holds outlives the delete `deleted`, the state's own being
/// `own`: an `lww` write when it is stamped after it, anything else when its state had
/// seen that very delete.
fn outlives(data: &Data, own: Stamp, deleted: Stamp) -> bool {
    match data {
        Data::Lww { stamp, .. } => *stamp > deleted,
        _ => own == deleted,
    }
}

/// Two things that both outlive the delete, as one, with its expiry.
fn join(a: Data, a_expires: u64, b: &Data, b_expires: u64) -> (Data, u64) {
    let ours = |a: Data| (a, a_expires);
    let theirs = || (b.clone(), b_expires);
    match (a, b) {
        (
            Data::Lww { stamp, value },
            Data::Lww {
                stamp: their_stamp,
                value: their_value,
            },
        ) => {
            let order = stamp
                .cmp(their_stamp)
                .then_with(|| exact_order(&value, their_value))
                .then_with(|| never_last(a_expires).cmp(&never_last(b_expires)));
            if order == Ordering::Less {
                theirs()
            } else {
                ours(Data::Lww { stamp, value })
            }
        }
        (a @ Data::Lww { .. }, _) => ours(a),
        (_, Data::Lww { .. }) => theirs(),
        (a, b) if a.tag() != b.tag() => {
            if a.tag() > b.tag() {
                ours(a)
            } else {
                theirs()
            }
        }
        (a, b) => {
            let joined = match (a, b) {
                (
                    Data::First { stamp, value },
                    Data::First {
                        stamp: their_stamp,
                        value: their_value,
                    },
                ) => {
                    let order = stamp
                        .cmp(their_stamp)
                        .then_with(|| exact_order(&value, their_value));
                    if order == Ordering::Greater {
                        b.clone()
                    } else {
                        Data::First { stamp, value }
                    }
                }
                (Data::Max(value), Data::Max(their_value)) => {
                    // The larger; of two equal numbers (`1` and `1.0`) the one `min`
                    // keeps too, so an integer is not replaced by its float.
                    let order = term_order(&value, their_value)
                        .then_with(|| exact_order(their_value, &value));
                    Data::Max(if order == Ordering::Less {
                        their_value.clone()
                    } else {
                        value
                    })
                }
                (Data::Min(value), Data::Min(their_value)) => {
                    Data::Min(if exact_order(&value, their_value) == Ordering::Greater {
                        their_value.clone()
                    } else {
                        value
                    })
                }
                (Data::Sum(mut counts), Data::Sum(their_counts)) => {
                    counts.merge(their_counts);
                    Data::Sum(counts)
                }
                (Data::Union(mut elements), Data::Union(their_elements)) => {
                    elements.merge(their_elements);
                    Data::Union(elements)
                }
                (Data::Hll(mut sketch), Data::Hll(their_sketch)) => {
                    sketch.merge(their_sketch);
                    Data::Hll(sketch)
                }
                // The tags are equal, so the types are.
                (a, _) => a,
            };
            (joined, later(a_expires, b_expires))
        }
    }
}

/// One key of a replicated namespace: the latest delete it has seen, what it holds
/// since, and when that expires. See the module's documentation for how two merge.
#[derive(Debug, Clone, Default)]
pub struct KeyState {
    /// The latest delete seen; [`Stamp::ZERO`] for none.
    deleted: Stamp,
    /// What it holds; `None` after a delete (a tombstone) or before any write.
    data: Option<Data>,
    /// When `data` expires, in milliseconds of the clock's physical part; `0`: never,
    /// and always `0` without `data`.
    expires_ms: u64,
}

/// A local write refused by the key it was for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteError {
    /// The key holds a value of another type.
    WrongType {
        /// The type it holds.
        held: &'static str,
    },
    /// The set is full.
    SetFull,
    /// The counter would pass 64 bits, or has counts of too many nodes.
    Counter(CountsError),
    /// The value cannot be replicated.
    Value(NotStorable),
}

/// One local write to a key.
#[derive(Debug, Clone, Copy)]
pub enum Write<'a> {
    /// `kv_set`: store the value (`lww`).
    Set(&'a Value),
    /// `kv_set_once`: store the value unless one is held (`first`).
    SetOnce(&'a Value),
    /// `kv_max` (`largest`) or `kv_min`: keep the larger or smaller number.
    Extreme {
        /// The number offered.
        value: &'a Value,
        /// `true`: `kv_max`.
        largest: bool,
    },
    /// `kv_incr`: add to the counter (`sum`).
    Incr(i64),
    /// `kv_add`: add an element to the set (`union`).
    Add(&'a Value),
    /// `kv_hll_add`: add the value with this [`stable_hash`] to the sketch (`hll`).
    HllAdd(u64),
    /// `kv_del`: delete the key, whatever its type.
    Delete,
}

/// Who writes, and when.
#[derive(Debug, Clone, Copy)]
pub struct Writer<'a> {
    /// The node's id: whose counts an increment raises.
    pub node: &'a str,
    /// The node's clock.
    pub clock: &'a Hlc,
    /// The wall clock, in milliseconds since the Unix epoch.
    pub wall_ms: u64,
}

impl KeyState {
    /// A state from its parts. `data` that `deleted` covers is dropped.
    #[must_use]
    pub fn new(deleted: Stamp, data: Option<Data>, expires_ms: u64) -> Self {
        let data = data.filter(|data| match data {
            Data::Lww { stamp, .. } | Data::First { stamp, .. } => *stamp > deleted,
            _ => true,
        });
        Self {
            deleted,
            expires_ms: if data.is_some() { expires_ms } else { 0 },
            data,
        }
    }

    /// The latest delete it has seen.
    #[must_use]
    pub fn deleted(&self) -> Stamp {
        self.deleted
    }

    /// What it holds, expired or not.
    #[must_use]
    pub fn data(&self) -> Option<&Data> {
        self.data.as_ref()
    }

    /// When what it holds expires, in milliseconds; `0`: never.
    #[must_use]
    pub fn expires_ms(&self) -> u64 {
        self.expires_ms
    }

    /// What it holds at `now_ms`: nothing once its expiry has passed.
    #[must_use]
    pub fn live(&self, now_ms: u64) -> Option<&Data> {
        self.data
            .as_ref()
            .filter(|_| self.expires_ms == 0 || self.expires_ms > now_ms)
    }

    /// The latest stamp in it: what a node must have accepted ([`Hlc::observe`])
    /// before it merges the state.
    #[must_use]
    pub fn latest(&self) -> Stamp {
        match &self.data {
            Some(Data::Lww { stamp, .. } | Data::First { stamp, .. }) => self.deleted.max(*stamp),
            _ => self.deleted,
        }
    }

    /// Whether nothing is lost by forgetting the key at `now_ms`: it holds nothing (or
    /// what it holds has expired) and its delete is older than `window_ms`, the
    /// namespace's tombstone window. Until then a tombstone is kept, so that a copy
    /// from before the delete cannot bring the key back.
    #[must_use]
    pub fn forgettable(&self, now_ms: u64, window_ms: u64) -> bool {
        self.live(now_ms).is_none() && self.deleted.ms().saturating_add(window_ms) <= now_ms
    }

    /// Make this the join of itself and `other`.
    pub fn merge(&mut self, other: &KeyState) {
        let deleted = self.deleted.max(other.deleted);
        let own = self.deleted;
        let mine = self.data.take().filter(|data| outlives(data, own, deleted));
        let theirs = other
            .data
            .as_ref()
            .filter(|data| outlives(data, other.deleted, deleted));
        self.deleted = deleted;
        match (mine, theirs) {
            (None, None) => self.expires_ms = 0,
            (Some(mine), None) => self.data = Some(mine),
            (None, Some(theirs)) => {
                self.data = Some(theirs.clone());
                self.expires_ms = other.expires_ms;
            }
            (Some(mine), Some(theirs)) => {
                let (data, expires_ms) = join(mine, self.expires_ms, theirs, other.expires_ms);
                self.data = Some(data);
                self.expires_ms = expires_ms;
            }
        }
    }

    /// A delete at `at`: a tombstone.
    fn delete(&mut self, at: Stamp) {
        self.deleted = self.deleted.max(at);
        self.data = None;
        self.expires_ms = 0;
    }

    /// One local write, as the node `by` makes it now. A write with `ttl_secs` sets
    /// the expiry; one without gives a new key `default_ttl_secs` and leaves an
    /// existing key's expiry as it is. A key found expired is deleted first, so what
    /// is written starts afresh on every node. Nothing but that changes when the write
    /// is refused.
    pub fn write(
        &mut self,
        write: &Write<'_>,
        ttl_secs: Option<u64>,
        default_ttl_secs: Option<u64>,
        by: &Writer<'_>,
    ) -> Result<(), WriteError> {
        self.apply(write, ttl_secs, default_ttl_secs, by)
            .map(|_| ())
    }

    /// [`write`](Self::write), answering whether the state is another than it was: a
    /// value that is already the largest, an element the set holds, a value the
    /// sketch has seen and a second `kv_set_once` leave it as it is, and there is
    /// then nothing to send to anyone.
    #[allow(clippy::too_many_lines)] // one arm per write and type
    pub fn apply(
        &mut self,
        write: &Write<'_>,
        ttl_secs: Option<u64>,
        default_ttl_secs: Option<u64>,
        by: &Writer<'_>,
    ) -> Result<bool, WriteError> {
        let now_ms = by.clock.physical_ms(by.wall_ms);
        let mut changed = false;
        if self.data.is_some() && self.live(now_ms).is_none() {
            self.delete(by.clock.now(by.wall_ms));
            changed = true;
        }
        let wrong = |held: &Data| WriteError::WrongType {
            held: held.type_name(),
        };
        let number = |value: &Value| {
            if value.is_number() {
                normal(value).map_err(WriteError::Value)
            } else {
                Err(WriteError::Value(NotStorable("not a number")))
            }
        };
        let new = self.data.is_none();
        match (write, &mut self.data) {
            (Write::Delete, _) => {
                self.delete(by.clock.now(by.wall_ms));
                return Ok(true);
            }
            (Write::Set(value), None | Some(Data::Lww { .. })) => {
                let value = normal(value).map_err(WriteError::Value)?;
                let stamp = by.clock.now(by.wall_ms);
                self.data = Some(Data::Lww { stamp, value });
                changed = true;
            }
            (Write::SetOnce(value), None) => {
                let value = normal(value).map_err(WriteError::Value)?;
                let stamp = by.clock.now(by.wall_ms);
                self.data = Some(Data::First { stamp, value });
                changed = true;
            }
            (Write::SetOnce(_), Some(Data::First { .. })) => {}
            (Write::Extreme { value, largest }, None) => {
                let value = number(value)?;
                self.data = Some(if *largest {
                    Data::Max(value)
                } else {
                    Data::Min(value)
                });
                changed = true;
            }
            (
                Write::Extreme {
                    value,
                    largest: true,
                },
                Some(Data::Max(held)),
            ) => {
                if term_order(value, held) == Ordering::Greater {
                    *held = number(value)?;
                    changed = true;
                }
            }
            (
                Write::Extreme {
                    value,
                    largest: false,
                },
                Some(Data::Min(held)),
            ) => {
                if term_order(value, held) == Ordering::Less {
                    *held = number(value)?;
                    changed = true;
                }
            }
            (Write::Incr(n), None) => {
                let mut counts = Counts::default();
                counts.add(by.node, *n).map_err(WriteError::Counter)?;
                self.data = Some(Data::Sum(counts));
                changed = true;
            }
            (Write::Incr(n), Some(Data::Sum(counts))) => {
                counts.add(by.node, *n).map_err(WriteError::Counter)?;
                changed |= *n != 0;
            }
            (Write::Add(element), slot @ None) => {
                let element = normal(element).map_err(WriteError::Value)?;
                let mut elements = Elements::default();
                elements
                    .insert(&term_key(&element), &element, Value::clone)
                    .map_err(|SetFull| WriteError::SetFull)?;
                *slot = Some(Data::Union(elements));
                changed = true;
            }
            (Write::Add(element), Some(Data::Union(elements))) => {
                let element = normal(element).map_err(WriteError::Value)?;
                changed |= elements
                    .insert(&term_key(&element), &element, Value::clone)
                    .map_err(|SetFull| WriteError::SetFull)?;
            }
            (Write::HllAdd(hash), slot @ None) => {
                let mut sketch = Box::<Hll>::default();
                sketch.add(*hash);
                *slot = Some(Data::Hll(sketch));
                changed = true;
            }
            (Write::HllAdd(hash), Some(Data::Hll(sketch))) => {
                changed |= sketch.add(*hash);
            }
            (_, Some(held)) => return Err(wrong(held)),
        }
        if let Some(secs) = ttl_secs.or(default_ttl_secs.filter(|_| new)) {
            let expires_ms = now_ms.saturating_add(secs.saturating_mul(1000)).max(1);
            changed |= expires_ms != self.expires_ms;
            self.expires_ms = expires_ms;
        }
        Ok(changed)
    }

    /// The state as bytes: at most [`MAX_ENCODED_BYTES`] of them, appended to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(VERSION);
        self.deleted.put(out);
        put_varint(u128::from(self.expires_ms), out);
        let Some(data) = &self.data else {
            out.push(0);
            return;
        };
        out.push(data.tag());
        match data {
            Data::Lww { stamp, value } | Data::First { stamp, value } => {
                stamp.put(out);
                put_value(value, out);
            }
            Data::Max(value) | Data::Min(value) => put_value(value, out),
            Data::Sum(counts) => counts.put(out),
            Data::Union(elements) => {
                put_len(elements.len(), out);
                for element in elements.iter() {
                    put_value(element, out);
                }
            }
            Data::Hll(sketch) => sketch.put(out),
        }
    }

    /// The state as bytes.
    #[must_use]
    pub fn encoded(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }

    /// The state `bytes` are, or why they are none. Total: it never panics, reads
    /// nothing past `bytes`, and allocates in proportion to them. What it accepts is
    /// what [`encode`](Self::encode) writes: encoding the result gives `bytes` back.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() > MAX_ENCODED_BYTES {
            return Err(DecodeError::TooLarge);
        }
        let mut reader = Reader { rest: bytes };
        match reader.byte()? {
            VERSION => {}
            other => return Err(DecodeError::Version(other)),
        }
        let deleted = reader.stamp()?;
        let expires_ms = reader.u64_varint()?;
        let number = |reader: &mut Reader<'_>| {
            let value = reader.value(MAX_VALUE_BYTES)?;
            if value.is_number() {
                Ok(value)
            } else {
                Err(DecodeError::Malformed("a maximum that is not a number"))
            }
        };
        let stamped = |reader: &mut Reader<'_>| {
            let stamp = reader.stamp()?;
            if stamp <= deleted {
                return Err(DecodeError::Malformed("a write its own delete covers"));
            }
            Ok((stamp, reader.value(MAX_VALUE_BYTES)?))
        };
        let data = match reader.byte()? {
            0 => None,
            1 => {
                let (stamp, value) = stamped(&mut reader)?;
                Some(Data::Lww { stamp, value })
            }
            2 => {
                let (stamp, value) = stamped(&mut reader)?;
                Some(Data::First { stamp, value })
            }
            3 => Some(Data::Max(number(&mut reader)?)),
            4 => Some(Data::Min(number(&mut reader)?)),
            5 => Some(Data::Sum(Counts::read(&mut reader)?)),
            6 => Some(Data::Union(Elements::read(&mut reader)?)),
            7 => Some(Data::Hll(Box::new(Hll::read(&mut reader)?))),
            _ => return Err(DecodeError::Malformed("an unknown type")),
        };
        if data.is_none() && expires_ms != 0 {
            return Err(DecodeError::Malformed("an expiry with nothing to expire"));
        }
        if !reader.rest.is_empty() {
            return Err(DecodeError::Malformed("bytes after the state"));
        }
        Ok(Self {
            deleted,
            data,
            expires_ms,
        })
    }
}

// -- a replica --------------------------------------------------------------------------

/// One node's copy of a replicated namespace, reduced to what decides whether copies
/// converge: its clock, its keys, local writes, states arriving from other nodes, and
/// forgetting what a delete left. No bounds, no accounting and no locks: those are the
/// store's. The property tests and the fuzz target run several of these against each
/// other.
#[derive(Debug)]
pub struct Replica {
    node: Box<str>,
    clock: Hlc,
    keys: BTreeMap<Vec<u8>, KeyState>,
}

impl Replica {
    /// The copy of the node `node`.
    #[must_use]
    pub fn new(node: &str) -> Self {
        Self {
            node: Box::from(node),
            clock: Hlc::new(node),
            keys: BTreeMap::new(),
        }
    }

    /// One local write to `key`, the wall clock reading `wall_ms`
    /// ([`KeyState::write`]).
    pub fn write(
        &mut self,
        key: &[u8],
        write: &Write<'_>,
        ttl_secs: Option<u64>,
        default_ttl_secs: Option<u64>,
        wall_ms: u64,
    ) -> Result<(), WriteError> {
        let by = Writer {
            node: &self.node,
            clock: &self.clock,
            wall_ms,
        };
        self.keys
            .entry(key.to_vec())
            .or_default()
            .write(write, ttl_secs, default_ttl_secs, &by)
    }

    /// Another node's state of `key` arrives. Refused, and nothing changes, when it
    /// carries a stamp too far ahead of this node's wall clock.
    pub fn receive(&mut self, key: &[u8], state: &KeyState, wall_ms: u64) -> Result<(), Skew> {
        self.clock.observe(state.latest(), wall_ms)?;
        self.keys.entry(key.to_vec()).or_default().merge(state);
        Ok(())
    }

    /// What `kv_get` reads from `key`.
    #[must_use]
    pub fn read(&self, key: &[u8], wall_ms: u64) -> Option<Value> {
        let now_ms = self.clock.physical_ms(wall_ms);
        self.keys.get(key)?.live(now_ms).map(Data::read)
    }

    /// The state of `key`, when this copy has one.
    #[must_use]
    pub fn state(&self, key: &[u8]) -> Option<&KeyState> {
        self.keys.get(key)
    }

    /// Every key this copy has a state of, tombstones included.
    pub fn states(&self) -> impl Iterator<Item = (&[u8], &KeyState)> {
        self.keys.iter().map(|(key, state)| (&**key, state))
    }

    /// Forget every key that holds nothing and whose delete is older than
    /// `window_ms` ([`KeyState::forgettable`]). How many went.
    pub fn forget(&mut self, wall_ms: u64, window_ms: u64) -> usize {
        let now_ms = self.clock.physical_ms(wall_ms);
        let before = self.keys.len();
        self.keys
            .retain(|_, state| !state.forgettable(now_ms, window_ms));
        before - self.keys.len()
    }
}

#[cfg(test)]
#[path = "tests_crdt.rs"]
mod tests;
