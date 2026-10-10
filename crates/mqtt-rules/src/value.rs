//! The value model rule SQL evaluates over.
//!
//! It mirrors the term shapes EMQX's rule engine works with, because its comparison,
//! concatenation and conversion rules are defined on those shapes:
//!
//! - [`Value::Undefined`] is a **missing** field (Erlang `undefined`). It is distinct
//!   from JSON `null` ([`Value::Null`]): `is_null(x)` is true only for the first, and
//!   `is_null_var(x)` for both.
//! - Text is [`Value::Str`]; a payload that is not valid UTF-8 is [`Value::Bin`]. Both
//!   are "binaries" to the SQL (they compare and concatenate byte-wise). A template that
//!   renders `${payload}` keeps arbitrary bytes exact; JSON can only hold text, so
//!   JSON-encoding bytes that are not UTF-8 writes U+FFFD for each broken sequence, as
//!   EMQX's encoder does ([`Value::to_json`]).
//! - Arrays and maps are reference counted, so selecting a decoded payload into several
//!   outputs clones a pointer, not the document. Maps keep insertion order, so
//!   `SELECT a, b` renders as `{"a":…,"b":…}`.

use bytes::Bytes;
use num_bigint::BigInt;
use std::cmp::Ordering;
use std::fmt::Write as _;
use std::sync::Arc;

use crate::num;
use crate::EvalError;

/// One rule-SQL value.
#[derive(Debug, Clone, Default)]
pub enum Value {
    /// A missing field or an unassigned variable.
    #[default]
    Undefined,
    /// JSON `null`.
    Null,
    /// A boolean.
    Bool(bool),
    /// An integer that fits 64 bits.
    Int(i64),
    /// An integer that does not fit 64 bits — never one that does (see [`crate::num`]).
    /// Erlang integers have no fixed width, so arithmetic that leaves `i64` continues
    /// here instead of overflowing.
    Big(Arc<BigInt>),
    /// A finite float. Operations that would produce NaN or infinity are errors, as
    /// they are on the Erlang VM EMQX runs on.
    Float(f64),
    /// UTF-8 text.
    Str(Arc<str>),
    /// Bytes that are not UTF-8 (a binary payload, a decoded base64 string).
    Bin(Bytes),
    /// An array.
    Array(Arc<Vec<Value>>),
    /// A map with string keys, in insertion order.
    Map(Arc<Map>),
}

/// An insertion-ordered string-keyed map.
///
/// A vector of pairs rather than a hash map: rule outputs and the JSON payloads rules
/// read are small, a linear scan over a few keys beats hashing them, and the order is
/// what the user wrote.
#[derive(Debug, Clone, Default)]
pub struct Map {
    entries: Vec<(Arc<str>, Value)>,
}

impl Map {
    /// An empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty map with room for `n` entries.
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            entries: Vec::with_capacity(n),
        }
    }

    /// The value under `key`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries
            .iter()
            .find(|(k, _)| &**k == key)
            .map(|(_, v)| v)
    }

    /// A mutable reference to the value under `key`.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.entries
            .iter_mut()
            .find(|(k, _)| &**k == key)
            .map(|(_, v)| v)
    }

    /// Set `key` to `value`, replacing in place (the key keeps its position) or
    /// appending.
    pub fn insert(&mut self, key: impl Into<Arc<str>>, value: Value) {
        let key = key.into();
        if let Some(slot) = self.get_mut(&key) {
            *slot = value;
        } else {
            self.entries.push((key, value));
        }
    }

    /// Remove `key`, returning its value.
    pub fn remove(&mut self, key: &str) -> Option<Value> {
        let at = self.entries.iter().position(|(k, _)| &**k == key)?;
        Some(self.entries.remove(at).1)
    }

    /// Number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entries, in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&Arc<str>, &Value)> {
        self.entries.iter().map(|(k, v)| (k, v))
    }

    /// Merge `other` into this map, `other` winning on a shared key.
    pub fn merge_from(&mut self, other: &Map) {
        if self.entries.len().saturating_mul(other.entries.len()) <= LINEAR_SCAN_WORK {
            for (k, v) in &other.entries {
                self.insert(k.clone(), v.clone());
            }
            return;
        }
        let mut pairs = std::mem::take(&mut self.entries);
        pairs.extend(other.entries.iter().cloned());
        *self = Self::from_pairs(pairs);
    }

    /// A map of `pairs` — a repeated key keeps its first position and its last value,
    /// exactly as repeated [`insert`](Self::insert)s would leave it — built in linear
    /// time. Inserting one by one scans the map each time, which for a decoded payload
    /// object with many keys is quadratic in a size the publisher chooses.
    #[must_use]
    pub fn from_pairs(pairs: Vec<(Arc<str>, Value)>) -> Self {
        if pairs.len() <= SMALL_MAP {
            let mut m = Self::with_capacity(pairs.len());
            for (k, v) in pairs {
                m.insert(k, v);
            }
            return m;
        }
        let mut at: std::collections::HashMap<Arc<str>, usize> =
            std::collections::HashMap::with_capacity(pairs.len());
        let mut entries: Vec<(Arc<str>, Value)> = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            if let Some(&i) = at.get(&k) {
                entries[i].1 = v;
            } else {
                at.insert(k.clone(), entries.len());
                entries.push((k, v));
            }
        }
        Self { entries }
    }
}

/// Up to this many entries, a linear scan per key beats building a hash index.
const SMALL_MAP: usize = 32;

/// Up to this many key comparisons, compare or merge two maps by scanning.
const LINEAR_SCAN_WORK: usize = SMALL_MAP * SMALL_MAP;

impl PartialEq for Map {
    /// Structural, order-independent equality (Erlang map equality).
    fn eq(&self, other: &Self) -> bool {
        self.eq_by(other, Value::loose_eq)
    }
}

impl Map {
    /// Order-independent equality, values compared by `same`.
    fn eq_by(&self, other: &Self, same: fn(&Value, &Value) -> bool) -> bool {
        if self.len() != other.len() {
            return false;
        }
        if self.len().saturating_mul(other.len()) <= LINEAR_SCAN_WORK {
            return self
                .entries
                .iter()
                .all(|(k, v)| other.get(k).is_some_and(|o| same(v, o)));
        }
        let index: std::collections::HashMap<&str, &Value> =
            other.entries.iter().map(|(k, v)| (&**k, v)).collect();
        self.entries
            .iter()
            .all(|(k, v)| index.get(&**k).is_some_and(|o| same(v, o)))
    }
}

impl FromIterator<(Arc<str>, Value)> for Map {
    fn from_iter<T: IntoIterator<Item = (Arc<str>, Value)>>(iter: T) -> Self {
        let mut m = Map::new();
        for (k, v) in iter {
            m.insert(k, v);
        }
        m
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Str(Arc::from(s))
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Str(Arc::from(s))
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int(n)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Map> for Value {
    fn from(m: Map) -> Self {
        Value::Map(Arc::new(m))
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Value::Array(Arc::new(v))
    }
}

impl Value {
    /// A payload's value: text when it is UTF-8, bytes otherwise.
    #[must_use]
    pub fn from_bytes(b: &Bytes) -> Self {
        match std::str::from_utf8(b) {
            Ok(s) => Value::Str(Arc::from(s)),
            Err(_) => Value::Bin(b.clone()),
        }
    }

    /// A float, or an error if it is not finite.
    pub fn float(f: f64) -> Result<Self, EvalError> {
        if f.is_finite() {
            Ok(Value::Float(f))
        } else {
            Err(EvalError::new("arithmetic produced a non-finite number"))
        }
    }

    /// The SQL type name, for error messages.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Undefined => "undefined",
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Int(_) | Value::Big(_) => "integer",
            Value::Float(_) => "float",
            Value::Str(_) => "string",
            Value::Bin(_) => "binary",
            Value::Array(_) => "array",
            Value::Map(_) => "map",
        }
    }

    /// Whether this is [`Value::Undefined`].
    #[must_use]
    pub fn is_undefined(&self) -> bool {
        matches!(self, Value::Undefined)
    }

    /// Whether this is exactly boolean `true` — the only value a condition passes on.
    #[must_use]
    pub fn is_true(&self) -> bool {
        matches!(self, Value::Bool(true))
    }

    /// The bytes of a string or binary.
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Str(s) => Some(s.as_bytes()),
            Value::Bin(b) => Some(b),
            _ => None,
        }
    }

    /// The text of a string (a binary only when it happens to be UTF-8).
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            Value::Bin(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// The number as `f64`, converted as the Erlang VM converts it; `None` for a
    /// non-number and for an integer too large for a float.
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        num::to_f64(self)
    }

    /// Whether this is a number.
    #[must_use]
    pub fn is_number(&self) -> bool {
        matches!(self, Value::Int(_) | Value::Big(_) | Value::Float(_))
    }

    /// Whether this is a string or binary.
    #[must_use]
    pub fn is_binary(&self) -> bool {
        matches!(self, Value::Str(_) | Value::Bin(_))
    }

    /// Erlang `==`: numbers compare by value (`1 == 1.0`, but
    /// `9007199254740993 == 9007199254740992.0` is false although both are the same
    /// `f64`), binaries byte-wise, containers structurally.
    #[must_use]
    pub fn loose_eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Undefined, Value::Undefined) | (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (a, b) if a.is_number() && b.is_number() => a.num_cmp(b) == Ordering::Equal,
            (a, b) if a.is_binary() && b.is_binary() => a.as_bytes() == b.as_bytes(),
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.loose_eq(y))
            }
            (Value::Map(a), Value::Map(b)) => a == b,
            _ => false,
        }
    }

    /// Erlang `=:=` (`IN`, `CASE x WHEN`, `contains`): like [`loose_eq`](Self::loose_eq)
    /// except that an integer never equals a float and `-0.0` is not `0.0`, at any depth.
    #[must_use]
    pub fn exact_eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::Float(_), _) | (_, Value::Float(_)) => false,
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.exact_eq(y))
            }
            (Value::Map(a), Value::Map(b)) => a.eq_by(b, Value::exact_eq),
            (a, b) => a.loose_eq(b),
        }
    }

    /// Two numbers by value.
    fn num_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (n, Value::Float(f)) => num::cmp_int_float(n, *f),
            (Value::Float(f), n) => num::cmp_int_float(n, *f).reverse(),
            (a, b) => match (num::big(a), num::big(b)) {
                (Some(a), Some(b)) => a.cmp(&b),
                _ => Ordering::Equal,
            },
        }
    }

    /// Erlang term order, used where EMQX compares two values of unrelated types:
    /// number < atom (boolean, null, undefined) < map < list < binary.
    fn rank(&self) -> u8 {
        match self {
            Value::Int(_) | Value::Big(_) | Value::Float(_) => 0,
            Value::Bool(_) | Value::Null | Value::Undefined => 1,
            Value::Map(_) => 2,
            Value::Array(_) => 3,
            Value::Str(_) | Value::Bin(_) => 4,
        }
    }

    /// The atom name of an atom-like value, for Erlang's alphabetical atom order and
    /// for EMQX's atom-versus-binary comparison.
    fn atom_name(&self) -> Option<&'static str> {
        match self {
            Value::Bool(true) => Some("true"),
            Value::Bool(false) => Some("false"),
            Value::Null => Some("null"),
            Value::Undefined => Some("undefined"),
            _ => None,
        }
    }

    /// A total order consistent with [`loose_eq`](Self::loose_eq) on equal types and
    /// with Erlang term order across types.
    #[must_use]
    pub fn term_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (a, b) if a.is_number() && b.is_number() => a.num_cmp(b),
            (a, b) if a.is_binary() && b.is_binary() => {
                a.as_bytes().unwrap_or(&[]).cmp(b.as_bytes().unwrap_or(&[]))
            }
            (a, b) if a.atom_name().is_some() && b.atom_name().is_some() => {
                a.atom_name().cmp(&b.atom_name())
            }
            (Value::Array(a), Value::Array(b)) => {
                for (x, y) in a.iter().zip(b.iter()) {
                    match x.term_cmp(y) {
                        Ordering::Equal => {}
                        o => return o,
                    }
                }
                a.len().cmp(&b.len())
            }
            (Value::Map(a), Value::Map(b)) => {
                // Erlang orders maps by size first; equal sizes fall back to the
                // structural comparison, which is all a rule can observe.
                a.len().cmp(&b.len()).then_with(|| {
                    if a == b {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                })
            }
            (a, b) => a.rank().cmp(&b.rank()),
        }
    }

    /// The value as text the way EMQX's `str/1` and its templates render it:
    /// binaries as-is, integers in decimal, floats with at most ten decimals and
    /// trailing zeros trimmed (an error for one too large for that, past 255
    /// characters), atoms by name, maps and arrays as JSON.
    pub fn to_text(&self) -> Result<String, EvalError> {
        Ok(match self {
            Value::Str(s) => s.to_string(),
            Value::Bin(b) => String::from_utf8_lossy(b).into_owned(),
            Value::Int(n) => n.to_string(),
            Value::Big(n) => n.to_string(),
            Value::Float(f) => format_float(*f)?,
            Value::Bool(_) | Value::Null | Value::Undefined => {
                self.atom_name().unwrap_or_default().to_string()
            }
            Value::Array(_) | Value::Map(_) => self.to_json()?,
        })
    }

    /// The value rendered as bytes for a template: like [`to_text`](Self::to_text),
    /// except a binary keeps its exact bytes.
    pub fn render_into(&self, out: &mut Vec<u8>) -> Result<(), EvalError> {
        match self {
            Value::Str(s) => out.extend_from_slice(s.as_bytes()),
            Value::Bin(b) => out.extend_from_slice(b),
            other => out.extend_from_slice(other.to_text()?.as_bytes()),
        }
        Ok(())
    }

    /// JSON text of this value, as EMQX's encoder (jiffy with `force_utf8`) writes it.
    ///
    /// Text is written as it is. When any string in the value is not valid UTF-8, jiffy
    /// repairs the whole value first (`jiffy_utf8:fix/1`) and so does this: each broken
    /// sequence becomes one U+FFFD ([`fix_utf8`]). It never fails; the `Result` is kept
    /// for its callers.
    pub fn to_json(&self) -> Result<String, EvalError> {
        let mut out = String::new();
        if !write_json(self, false, &mut out) {
            out.clear();
            write_json(self, true, &mut out);
        }
        Ok(out)
    }
}

/// EMQX's float text: `float_to_binary(F, [{decimals, 10}, compact])` — fixed
/// notation, at most ten decimals (a tie rounded away from zero), trailing zeros trimmed
/// but one kept, the sign of `-0.0` kept. Like Erlang's, it fails past 255 characters.
pub fn format_float(f: f64) -> Result<String, EvalError> {
    num::float_to_decimals(f, 10, true)
}

/// A JSON string as jiffy writes one: `\"`, `\\`, `\b`, `\f`, `\n`, `\r`, `\t`, any other
/// character below U+0020 as `\u00XX` with upper-case hex digits, and everything else —
/// `/`, DEL, U+2028 — as itself.
fn write_json_str(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Whether jiffy's repair keeps code point `c` (`xmerl_ucs:is_unicode/1`): not a
/// surrogate, not U+FFFE or U+FFFF, not past U+10FFFF.
fn is_unicode(c: u32) -> bool {
    !matches!(c, 0xD800..=0xDFFF | 0xFFFE | 0xFFFF) && c < 0x11_0000
}

/// Bytes as the text jiffy's `force_utf8` repair (`jiffy_utf8:fix_bin/1`) makes of them.
///
/// It decodes loosely — a lead byte with all its continuation bytes is a code point,
/// overlong forms included (`C0 80` is U+0000) — and replaces any other byte, together
/// with every continuation byte that follows it, by one U+FFFD. Two adjacent surrogates
/// that form a UTF-16 pair are joined (CESU-8), taken two at a time from the left. A
/// code point that is not a character (a lone surrogate, U+FFFE, U+FFFF, past U+10FFFF)
/// becomes U+FFFD. Valid UTF-8 comes back unchanged, except U+FFFE and U+FFFF.
fn fix_utf8(bytes: &[u8]) -> String {
    const REPLACEMENT: u32 = 0xFFFD;
    let cont = |i: usize| {
        bytes
            .get(i)
            .filter(|c| **c & 0xC0 == 0x80)
            .map(|c| u32::from(c & 0x3F))
    };
    let mut points: Vec<u32> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while let Some(&c) = bytes.get(i) {
        let lead = u32::from(c);
        let (point, len) = match (c, cont(i + 1), cont(i + 2), cont(i + 3)) {
            (0..=0x7F, ..) => (lead, 1),
            (0xC0..=0xDF, Some(c1), ..) => ((lead & 0x1F) << 6 | c1, 2),
            (0xE0..=0xEF, Some(c1), Some(c2), _) => ((lead & 0x0F) << 12 | c1 << 6 | c2, 3),
            (0xF0..=0xF7, Some(c1), Some(c2), Some(c3)) => {
                ((lead & 0x07) << 18 | c1 << 12 | c2 << 6 | c3, 4)
            }
            _ => {
                let mut len = 1;
                while cont(i + len).is_some() {
                    len += 1;
                }
                (REPLACEMENT, len)
            }
        };
        points.push(point);
        i += len;
    }
    let surrogate = |c: u32| (0xD800..=0xDFFF).contains(&c);
    let mut out = String::with_capacity(bytes.len());
    let mut i = 0;
    while let Some(&c) = points.get(i) {
        i += 1;
        let c = match points.get(i) {
            Some(&low) if surrogate(c) && surrogate(low) => {
                // The pair is consumed whether or not it joins.
                i += 1;
                if c < 0xDC00 && low >= 0xDC00 {
                    0x1_0000 + ((c - 0xD800) << 10) + (low - 0xDC00)
                } else {
                    out.push(char::REPLACEMENT_CHARACTER);
                    REPLACEMENT
                }
            }
            _ => c,
        };
        out.push(
            char::from_u32(c)
                .filter(|_| is_unicode(c))
                .unwrap_or(char::REPLACEMENT_CHARACTER),
        );
    }
    out
}

/// Write `s`, a string or a key, as JSON; repaired first when `fix` is set.
fn write_json_text(s: &str, fix: bool, out: &mut String) {
    if fix && s.contains(['\u{FFFE}', '\u{FFFF}']) {
        write_json_str(&fix_utf8(s.as_bytes()), out);
    } else {
        write_json_str(s, out);
    }
}

/// Write `v` as JSON. Without `fix`, `false` as soon as a string that is not UTF-8 is
/// met (jiffy's `invalid_string`), with `out` left partly written; with `fix`, every
/// string is written repaired ([`fix_utf8`]) and the answer is always `true`.
fn write_json(v: &Value, fix: bool, out: &mut String) -> bool {
    match v {
        Value::Undefined => write_json_str("undefined", out),
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(n) => {
            let _ = write!(out, "{n}");
        }
        Value::Big(n) => {
            let _ = write!(out, "{n}");
        }
        // jiffy's float: Erlang's shortest form (`1.0e20`, `0.1`), and `-0.0` as `0.0`.
        Value::Float(f) => out.push_str(&num::short_float(if *f == 0.0 { 0.0 } else { *f })),
        Value::Str(s) => write_json_text(s, fix, out),
        Value::Bin(b) => match std::str::from_utf8(b) {
            Ok(s) => write_json_text(s, fix, out),
            Err(_) if fix => write_json_str(&fix_utf8(b), out),
            Err(_) => return false,
        },
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                if !write_json(x, fix, out) {
                    return false;
                }
            }
            out.push(']');
        }
        Value::Map(m) => {
            out.push('{');
            for (i, (k, x)) in m.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_text(k, fix, out);
                out.push(':');
                if !write_json(x, fix, out) {
                    return false;
                }
            }
            out.push('}');
        }
    }
    true
}

/// Decode JSON text into a [`Value`] as EMQX (jiffy) does, preserving object key order:
/// an integer of any size stays an exact integer, a float reads as the nearest double.
pub fn json_decode(input: &[u8]) -> Result<Value, EvalError> {
    crate::json::decode(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floats_render_like_emqx_str() {
        // The examples from EMQX's `str/1` reference.
        let f = |x| format_float(x).unwrap();
        assert_eq!(f(0.300_000_000_40), "0.3000000004");
        assert_eq!(f(0.300_000_000_04), "0.3");
        assert_eq!(f(12.345_678_901_234), "12.3456789012");
        assert_eq!(f(0.000_000_314_159_265_359), "0.0000003142");
        assert_eq!(f(20.0), "20.0");
    }

    #[test]
    fn json_round_trip_keeps_key_order_and_types() {
        let v =
            json_decode(br#"{"b":1,"a":[true,null,2.5,"x"],"big":18446744073709551615}"#).unwrap();
        let Value::Map(m) = &v else { panic!() };
        let keys: Vec<&str> = m.iter().map(|(k, _)| &**k).collect();
        assert_eq!(keys, ["b", "a", "big"]);
        assert!(matches!(m.get("big"), Some(Value::Big(_))));
        assert_eq!(
            v.to_json().unwrap(),
            r#"{"b":1,"a":[true,null,2.5,"x"],"big":18446744073709551615}"#
        );
    }

    /// EMQX 6.3.1, `emqx_utils_json:encode/1` (jiffy 2.0.1 with `force_utf8`) of each
    /// binary: one U+FFFD per broken sequence, by `jiffy_utf8:fix_bin/1`'s loose decoding.
    /// A template still renders the bytes themselves.
    #[test]
    fn json_of_bytes_that_are_not_utf8_is_repaired_as_jiffy_repairs_it() {
        let json = |b: &[u8]| Value::Bin(Bytes::copy_from_slice(b)).to_json().unwrap();
        for (bytes, want) in [
            // `sprintf('~c', 210)`.
            (&[0xD2][..], "\"\u{FFFD}\""),
            (&[b'a', 0xD2, b'b'], "\"a\u{FFFD}b\""),
            (&[0xD2, 0xD2], "\"\u{FFFD}\u{FFFD}\""),
            // A sequence cut short is one replacement, with or without more text.
            (&[0xE2, 0x82], "\"\u{FFFD}\""),
            (&[0xE2, 0x82, b'a'], "\"\u{FFFD}a\""),
            (&[0xF0, 0x9F, 0x98], "\"\u{FFFD}\""),
            (&[0xF0, 0x9F, 0x98, b'a'], "\"\u{FFFD}a\""),
            (&[b'a', 0xC3], "\"a\u{FFFD}\""),
            (&[0xC3, b'a'], "\"\u{FFFD}a\""),
            (&[0xE2, 0x28, 0xA1], "\"\u{FFFD}(\u{FFFD}\""),
            // A stray byte takes every continuation byte after it along.
            (&[0x80], "\"\u{FFFD}\""),
            (&[0x80, 0x80, 0x80, 0x80, b'a'], "\"\u{FFFD}a\""),
            (&[0xF8, 0x88, 0x80, 0x80, 0x80], "\"\u{FFFD}\""),
            (&[0xFF, 0xFE], "\"\u{FFFD}\u{FFFD}\""),
            // Overlong forms decode: to U+0000, `A` and `/`.
            (&[0xC0, 0x80], "\"\\u0000\""),
            (&[0xE0, 0x80, 0x80], "\"\\u0000\""),
            (&[0xC1, 0x81], "\"A\""),
            (&[0xF0, 0x80, 0x80, 0xAF], "\"/\""),
            // Past U+10FFFF, and surrogates: alone, as a CESU-8 pair, and out of step.
            (&[0xF4, 0x90, 0x80, 0x80], "\"\u{FFFD}\""),
            (&[0xF7, 0xBF, 0xBF, 0xBF], "\"\u{FFFD}\""),
            (&[0xED, 0xA0, 0x80], "\"\u{FFFD}\""),
            (&[0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80], "\"\u{1F600}\""),
            (
                &[0xED, 0xB8, 0x80, 0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80],
                "\"\u{FFFD}\u{FFFD}\u{FFFD}\"",
            ),
            (
                &[0xED, 0xA0, 0xBD, 0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80],
                "\"\u{FFFD}\u{FFFD}\u{FFFD}\"",
            ),
            // Escapes are the same after a repair.
            (
                &[8, 12, 10, 27, 31, 34, 47, 92, 127, 0xD2],
                "\"\\b\\f\\n\\u001B\\u001F\\\"/\\\\\u{7F}\u{FFFD}\"",
            ),
        ] {
            assert_eq!(json(bytes), want, "{bytes:02X?}");
        }
        // A template renders the bytes, not their JSON (`emqx_template:render/2`).
        let mut out = Vec::new();
        Value::Bin(Bytes::from_static(&[0xD2]))
            .render_into(&mut out)
            .unwrap();
        assert_eq!(out, [0xD2]);
    }

    /// EMQX 6.3.1: jiffy repairs the whole value once any string in it is not UTF-8, and
    /// the repair also replaces U+FFFE and U+FFFF, which are written as themselves
    /// otherwise: `encode([<<"\xEF\xBF\xBE">>, <<"\xEF\xBF\xBF">>, <<210>>])` is three
    /// U+FFFD, `encode(<<"\xEF\xBF\xBE">>)` is U+FFFE. Other non-characters (U+FDD0,
    /// U+1FFFE) and numbers are untouched.
    #[test]
    fn a_json_repair_covers_the_whole_value() {
        let bad = Value::Bin(Bytes::from_static(&[0xD2]));
        assert_eq!(
            Value::from("\u{FFFE}\u{FFFF}").to_json().unwrap(),
            "\"\u{FFFE}\u{FFFF}\""
        );
        let all = Value::from(vec![
            Value::from("\u{FFFE}"),
            Value::from("\u{FFFF}"),
            Value::from("\u{FDD0}\u{1FFFE}"),
            Value::Float(1e20),
            json_decode(b"12345678901234567890").unwrap(),
            bad,
        ]);
        assert_eq!(
            all.to_json().unwrap(),
            "[\"\u{FFFD}\",\"\u{FFFD}\",\"\u{FDD0}\u{1FFFE}\",1.0e20,12345678901234567890,\"\u{FFFD}\"]"
        );
        // Keys too, and a map after the string that needed the repair.
        let mut m = Map::new();
        m.insert("k", Value::Bin(Bytes::from_static(&[1, 0xFF, 0xC4])));
        m.insert("\u{FFFF}", Value::Undefined);
        assert_eq!(
            Value::from(m).to_json().unwrap(),
            "{\"k\":\"\\u0001\u{FFFD}\u{FFFD}\",\"\u{FFFD}\":\"undefined\"}"
        );
    }

    /// EMQX 6.3.1, `emqx_utils_json:encode(<<0,1,7,8,9,10,11,12,13,14,27,31,32,34,47,92,
    /// 127>>)`: the short escapes, `\u00XX` in upper case for the other controls, and
    /// `/`, DEL and U+2028 as themselves.
    #[test]
    fn json_strings_escape_as_jiffy_escapes_them() {
        let s = "\0\u{1}\u{7}\u{8}\t\n\u{b}\u{c}\r\u{e}\u{1b}\u{1f} \"/\\\u{7f}\u{2028}";
        assert_eq!(
            Value::from(s).to_json().unwrap(),
            "\"\\u0000\\u0001\\u0007\\b\\t\\n\\u000B\\f\\r\\u000E\\u001B\\u001F \\\"/\\\\\u{7f}\u{2028}\""
        );
    }

    #[test]
    fn numbers_compare_across_int_and_float() {
        assert!(Value::Int(1).loose_eq(&Value::Float(1.0)));
        assert_eq!(
            Value::Int(2).term_cmp(&Value::Float(1.5)),
            Ordering::Greater
        );
        // number < atom < binary
        assert_eq!(Value::Int(9).term_cmp(&Value::Bool(false)), Ordering::Less);
        assert_eq!(
            Value::from("a").term_cmp(&Value::Bool(true)),
            Ordering::Greater
        );
    }
}
