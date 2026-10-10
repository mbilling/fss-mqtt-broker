//! JSON text to a [`Value`], as EMQX's decoder (jiffy) reads it.
//!
//! A hand-written reader rather than `serde_json` for one reason: numbers. jiffy keeps an
//! integer of any size exact (Erlang integers have no width), where a 64-bit reader can
//! only hand `12345678901234567890` over as the float `1.2345678901234567e19`; serde's
//! one escape from that (`arbitrary_precision`) changes `serde_json::Number` for every
//! crate in the build. Everything else follows RFC 8259 strictly, as serde does (whose
//! error messages this reader keeps, so a failure reads the same as before):
//!
//! - an integer is exact up to [`MAX_INT_BITS`](crate::num::MAX_INT_BITS) — the one
//!   bound jiffy does not have, refused (the payload is then not JSON to the rule)
//!   before it costs any work;
//! - a float is the double nearest its text (as jiffy's reader rounds), and one that
//!   would be infinite is an error, as in jiffy; one too small for a double is a zero
//!   keeping its sign;
//! - objects keep their key order; a repeated key keeps its first position and its
//!   last value;
//! - nesting deeper than 127 arrays/objects is refused (serde's limit), so decoding,
//!   rendering and dropping a payload never recurse without bound.

use std::sync::Arc;

use crate::num;
use crate::value::{Map, Value};
use crate::EvalError;

/// Nesting at which a document is refused, counting from the top level.
const MAX_DEPTH: u32 = 128;

/// Decode `input` as one JSON value.
pub(crate) fn decode(input: &[u8]) -> Result<Value, EvalError> {
    let mut r = Reader {
        b: input,
        i: 0,
        depth: MAX_DEPTH,
    };
    let v = r.value().map_err(|e| r.error(e))?;
    r.skip_ws();
    if r.i < r.b.len() {
        r.i += 1;
        return Err(r.error(Err::TrailingCharacters));
    }
    Ok(v)
}

/// What went wrong, worded as `serde_json` words it.
#[derive(Debug, Clone, Copy)]
enum Err {
    EofList,
    EofObject,
    EofString,
    EofValue,
    ExpectedColon,
    ExpectedListCommaOrEnd,
    ExpectedObjectCommaOrEnd,
    ExpectedIdent,
    ExpectedValue,
    InvalidEscape,
    InvalidNumber,
    NumberOutOfRange,
    IntegerTooLarge,
    InvalidUnicode,
    ControlCharacter,
    KeyMustBeAString,
    LoneLeadingSurrogate,
    TrailingComma,
    TrailingCharacters,
    UnexpectedEndOfHexEscape,
    RecursionLimit,
}

impl Err {
    fn text(self) -> String {
        match self {
            Err::EofList => "EOF while parsing a list".into(),
            Err::EofObject => "EOF while parsing an object".into(),
            Err::EofString => "EOF while parsing a string".into(),
            Err::EofValue => "EOF while parsing a value".into(),
            Err::ExpectedColon => "expected `:`".into(),
            Err::ExpectedListCommaOrEnd => "expected `,` or `]`".into(),
            Err::ExpectedObjectCommaOrEnd => "expected `,` or `}`".into(),
            Err::ExpectedIdent => "expected ident".into(),
            Err::ExpectedValue => "expected value".into(),
            Err::InvalidEscape => "invalid escape".into(),
            Err::InvalidNumber => "invalid number".into(),
            Err::NumberOutOfRange => "number out of range".into(),
            Err::IntegerTooLarge => format!(
                "integer too large (integers are limited to {} bits)",
                num::MAX_INT_BITS
            ),
            Err::InvalidUnicode => "invalid unicode code point".into(),
            Err::ControlCharacter => {
                "control character (\\u0000-\\u001F) found while parsing a string".into()
            }
            Err::KeyMustBeAString => "key must be a string".into(),
            Err::LoneLeadingSurrogate => "lone leading surrogate in hex escape".into(),
            Err::TrailingComma => "trailing comma".into(),
            Err::TrailingCharacters => "trailing characters".into(),
            Err::UnexpectedEndOfHexEscape => "unexpected end of hex escape".into(),
            Err::RecursionLimit => "recursion limit exceeded".into(),
        }
    }
}

type R<T> = Result<T, Err>;

struct Reader<'a> {
    b: &'a [u8],
    /// The next byte to read; an error is reported at the byte before it.
    i: usize,
    /// How many more arrays/objects may open.
    depth: u32,
}

impl Reader<'_> {
    /// The error at the current position: line and column (bytes) of the last byte read.
    // Only on the error path, so a plain count (no `bytecount` dependency).
    #[allow(clippy::naive_bytecount)]
    fn error(&self, e: Err) -> EvalError {
        let upto = &self.b[..self.i.min(self.b.len())];
        let line = 1 + upto.iter().filter(|c| **c == b'\n').count();
        let column = upto.len() - upto.iter().rposition(|c| *c == b'\n').map_or(0, |p| p + 1);
        EvalError::new(format!(
            "invalid JSON: {} at line {line} column {column}",
            e.text()
        ))
    }

    fn skip_ws(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.b.get(self.i) {
            self.i += 1;
        }
    }

    /// The next non-whitespace byte, not consumed.
    fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.b.get(self.i).copied()
    }

    /// Fail at the peeked byte (it counts as read).
    fn fail_here<T>(&mut self, e: Err) -> R<T> {
        self.i += 1;
        Err(e)
    }

    fn value(&mut self) -> R<Value> {
        match self.peek() {
            None => Err(Err::EofValue),
            Some(b'"') => {
                self.i += 1;
                Ok(Value::Str(self.string()?))
            }
            Some(b'{') => self.nested(Self::object),
            Some(b'[') => self.nested(Self::array),
            Some(b't') => self.ident(b"true", Value::Bool(true)),
            Some(b'f') => self.ident(b"false", Value::Bool(false)),
            Some(b'n') => self.ident(b"null", Value::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => self.fail_here(Err::ExpectedValue),
        }
    }

    fn nested(&mut self, f: fn(&mut Self) -> R<Value>) -> R<Value> {
        self.depth -= 1;
        if self.depth == 0 {
            return self.fail_here(Err::RecursionLimit);
        }
        self.i += 1;
        let v = f(self);
        self.depth += 1;
        v
    }

    fn ident(&mut self, word: &[u8], v: Value) -> R<Value> {
        for &c in word {
            match self.b.get(self.i) {
                None => return Err(Err::EofValue),
                Some(&got) => {
                    self.i += 1;
                    if got != c {
                        return Err(Err::ExpectedIdent);
                    }
                }
            }
        }
        Ok(v)
    }

    fn array(&mut self) -> R<Value> {
        let mut items = Vec::new();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(Value::from(items));
        }
        loop {
            items.push(self.value()?);
            match self.peek() {
                None => return Err(Err::EofList),
                Some(b']') => {
                    self.i += 1;
                    return Ok(Value::from(items));
                }
                Some(b',') => {
                    self.i += 1;
                    match self.peek() {
                        Some(b']') => return self.fail_here(Err::TrailingComma),
                        None => return Err(Err::EofValue),
                        Some(_) => {}
                    }
                }
                Some(_) => return self.fail_here(Err::ExpectedListCommaOrEnd),
            }
        }
    }

    fn object(&mut self) -> R<Value> {
        let mut pairs: Vec<(Arc<str>, Value)> = Vec::new();
        match self.peek() {
            Some(b'}') => {
                self.i += 1;
                return Ok(Value::from(Map::new()));
            }
            None => return Err(Err::EofObject),
            Some(_) => {}
        }
        loop {
            match self.peek() {
                Some(b'"') => self.i += 1,
                None => return Err(Err::EofValue),
                Some(_) => return self.fail_here(Err::KeyMustBeAString),
            }
            let key = self.string()?;
            match self.peek() {
                Some(b':') => self.i += 1,
                None => return Err(Err::EofObject),
                Some(_) => return self.fail_here(Err::ExpectedColon),
            }
            let v = self.value()?;
            pairs.push((key, v));
            match self.peek() {
                None => return Err(Err::EofObject),
                Some(b'}') => {
                    self.i += 1;
                    return Ok(Value::from(Map::from_pairs(pairs)));
                }
                Some(b',') => {
                    self.i += 1;
                    match self.peek() {
                        Some(b'}') => return self.fail_here(Err::TrailingComma),
                        None => return Err(Err::EofValue),
                        Some(_) => {}
                    }
                }
                Some(_) => return self.fail_here(Err::ExpectedObjectCommaOrEnd),
            }
        }
    }

    /// The rest of a string whose opening quote has been read.
    fn string(&mut self) -> R<Arc<str>> {
        let start = self.i;
        // Fast path: no escapes.
        loop {
            match self.b.get(self.i) {
                None => return Err(Err::EofString),
                Some(b'"') => {
                    self.i += 1;
                    let s = utf8(&self.b[start..self.i - 1]).map_err(|_| Err::InvalidUnicode)?;
                    return Ok(Arc::from(s));
                }
                Some(b'\\') => break,
                Some(c) if *c < 0x20 => return self.fail_here(Err::ControlCharacter),
                Some(_) => self.i += 1,
            }
        }
        let mut out: Vec<u8> = self.b[start..self.i].to_vec();
        loop {
            match self.b.get(self.i) {
                None => return Err(Err::EofString),
                Some(b'"') => {
                    self.i += 1;
                    let s = String::from_utf8(out).map_err(|_| Err::InvalidUnicode)?;
                    return Ok(Arc::from(s));
                }
                Some(b'\\') => {
                    self.i += 1;
                    self.escape(&mut out)?;
                }
                Some(c) if *c < 0x20 => return self.fail_here(Err::ControlCharacter),
                Some(c) => {
                    out.push(*c);
                    self.i += 1;
                }
            }
        }
    }

    /// The escape after a `\`.
    fn escape(&mut self, out: &mut Vec<u8>) -> R<()> {
        let Some(&c) = self.b.get(self.i) else {
            return Err(Err::EofString);
        };
        self.i += 1;
        let simple = match c {
            b'"' => b'"',
            b'\\' => b'\\',
            b'/' => b'/',
            b'b' => 0x08,
            b'f' => 0x0c,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'u' => {
                let ch = self.unicode_escape()?;
                let mut buf = [0u8; 4];
                out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                return Ok(());
            }
            _ => return Err(Err::InvalidEscape),
        };
        out.push(simple);
        Ok(())
    }

    /// The character of a `\uXXXX` escape (the `\u` read), joining a surrogate pair.
    fn unicode_escape(&mut self) -> R<char> {
        let hi = self.hex4()?;
        if !(0xD800..0xE000).contains(&hi) {
            return char::from_u32(u32::from(hi)).ok_or(Err::InvalidUnicode);
        }
        if hi >= 0xDC00 {
            return Err(Err::LoneLeadingSurrogate);
        }
        for want in *b"\\u" {
            let Some(&c) = self.b.get(self.i) else {
                return Err(Err::EofString);
            };
            self.i += 1;
            if c != want {
                return Err(Err::UnexpectedEndOfHexEscape);
            }
        }
        let lo = self.hex4()?;
        if !(0xDC00..0xE000).contains(&lo) {
            return Err(Err::LoneLeadingSurrogate);
        }
        let c = 0x1_0000 + ((u32::from(hi) - 0xD800) << 10) + (u32::from(lo) - 0xDC00);
        char::from_u32(c).ok_or(Err::InvalidUnicode)
    }

    /// Four hex digits, all read before any is checked.
    fn hex4(&mut self) -> R<u16> {
        let Some(four) = self.b.get(self.i..self.i + 4) else {
            self.i = self.b.len();
            return Err(Err::EofString);
        };
        self.i += 4;
        four.iter().try_fold(0u16, |n, &c| {
            let d = char::from(c).to_digit(16).ok_or(Err::InvalidEscape)?;
            Ok(n * 16 + u16::try_from(d).unwrap_or(0))
        })
    }

    /// A number: `-? (0 | [1-9][0-9]*) (. [0-9]+)? ([eE] [+-]? [0-9]+)?`.
    fn number(&mut self) -> R<Value> {
        let start = self.i;
        if self.b.get(self.i) == Some(&b'-') {
            self.i += 1;
        }
        let int_start = self.i;
        match self.b.get(self.i) {
            Some(b'0') => {
                self.i += 1;
                if self.b.get(self.i).is_some_and(u8::is_ascii_digit) {
                    return self.fail_here(Err::InvalidNumber);
                }
            }
            Some(b'1'..=b'9') => {
                while self.b.get(self.i).is_some_and(u8::is_ascii_digit) {
                    self.i += 1;
                }
            }
            None => return Err(Err::EofValue),
            Some(_) => return self.fail_here(Err::InvalidNumber),
        }
        let int_end = self.i;
        let mut float = false;
        if self.b.get(self.i) == Some(&b'.') {
            float = true;
            self.i += 1;
            if !self.digits()? {
                return self.fail_here(Err::InvalidNumber);
            }
        }
        if matches!(self.b.get(self.i), Some(b'e' | b'E')) {
            float = true;
            self.i += 1;
            if matches!(self.b.get(self.i), Some(b'+' | b'-')) {
                self.i += 1;
            }
            if !self.digits()? {
                return self.fail_here(Err::InvalidNumber);
            }
        }
        let text = &self.b[start..self.i];
        if float {
            // ASCII by construction; Rust's reader rounds to nearest, as jiffy's does.
            return utf8(text)
                .ok()
                .and_then(|t| t.parse::<f64>().ok())
                .filter(|f| f.is_finite())
                .map(Value::Float)
                .ok_or(Err::NumberOutOfRange);
        }
        if int_end - int_start <= 18 {
            // At most 18 digits: always an i64.
            let n = self.b[int_start..int_end]
                .iter()
                .fold(0i64, |n, d| n * 10 + i64::from(d - b'0'));
            return Ok(Value::Int(if start == int_start { n } else { -n }));
        }
        // Refused by its digit count before it is parsed (parsing is quadratic).
        match num::parse_int(text) {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Err(Err::InvalidNumber),
            Err(_) => Err(Err::IntegerTooLarge),
        }
    }

    /// Digits, at least one; `Ok(false)` when there are none.
    fn digits(&mut self) -> R<bool> {
        let start = self.i;
        while self.b.get(self.i).is_some_and(u8::is_ascii_digit) {
            self.i += 1;
        }
        if self.i == start && self.i >= self.b.len() {
            return Err(Err::EofValue);
        }
        Ok(self.i > start)
    }
}

fn utf8(b: &[u8]) -> Result<&str, std::str::Utf8Error> {
    std::str::from_utf8(b)
}
