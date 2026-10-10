//! Numbers as EMQX has them: Erlang integers, which never overflow, and IEEE doubles,
//! converted, compared, parsed and printed the way the Erlang VM (and jiffy, the JSON
//! library EMQX decodes and encodes with) does.
//!
//! An integer that fits 64 bits is [`Value::Int`]; any other is [`Value::Big`] — never
//! one that fits, so the common case stays a plain `i64` and two equal integers always
//! have the same shape. Every constructor of a [`Value::Big`] goes through [`from_big`].
//!
//! **The one bound.** An integer is at most [`MAX_INT_BITS`] bits. Erlang has no such
//! limit short of memory, but the cost of printing, dividing or parsing a big integer
//! grows with the square of its size, and a payload (or a `bitsl` whose shift is a
//! payload field) chooses that size; past the bound the operation fails, and so does the
//! rule. No integer a device sends — a 128-bit ID, a 256-bit hash — comes near it.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::Write as _;
use std::sync::Arc;

use num_bigint::{BigInt, Sign};
use num_traits::{FromPrimitive, Signed, ToPrimitive, Zero};

use crate::value::Value;
use crate::EvalError;

/// The widest integer, in bits of magnitude (2,466 decimal digits).
pub(crate) const MAX_INT_BITS: u64 = 8192;

/// The most decimal digits an integer of at most [`MAX_INT_BITS`] bits can have; text
/// with more is refused before it is parsed (parsing is quadratic in the digits).
pub(crate) const MAX_INT_DIGITS: usize = 2467;

pub(crate) fn too_large() -> EvalError {
    EvalError::new(format!(
        "integer too large: integers are limited to {MAX_INT_BITS} bits"
    ))
}

/// An integer as a [`Value`]: [`Value::Int`] when it fits 64 bits, [`Value::Big`]
/// otherwise, or an error past [`MAX_INT_BITS`].
pub(crate) fn from_big(b: BigInt) -> Result<Value, EvalError> {
    if let Some(n) = b.to_i64() {
        return Ok(Value::Int(n));
    }
    if b.bits() > MAX_INT_BITS {
        return Err(too_large());
    }
    Ok(Value::Big(Arc::new(b)))
}

/// The integer in an integer value.
pub(crate) fn big(v: &Value) -> Option<Cow<'_, BigInt>> {
    match v {
        Value::Int(n) => Some(Cow::Owned(BigInt::from(*n))),
        Value::Big(b) => Some(Cow::Borrowed(b)),
        _ => None,
    }
}

/// Whether this is an integer.
pub(crate) fn is_int(v: &Value) -> bool {
    matches!(v, Value::Int(_) | Value::Big(_))
}

/// An integer converted to a float as the Erlang VM converts it (`big_to_double`): the
/// 64-bit digits folded in from the most significant, `d * 2^64 + digit`, rounding at
/// each step — so not always the nearest float — and `None` when it is not finite
/// (`badarith` in Erlang).
pub(crate) fn big_to_f64(b: &BigInt) -> Option<f64> {
    let (sign, digits) = b.to_u64_digits();
    let mut d = 0.0f64;
    for &x in digits.iter().rev() {
        // The digit's conversion rounds to nearest, as C's `(double)` does.
        #[allow(clippy::cast_precision_loss)]
        let x = x as f64;
        d = d * 18_446_744_073_709_551_616.0 + x;
        if !d.is_finite() {
            return None;
        }
    }
    Some(if sign == Sign::Minus { -d } else { d })
}

/// A number as a float, as Erlang's arithmetic and `math` functions take it; `None` for
/// a non-number or an integer too large for a float.
pub(crate) fn to_f64(v: &Value) -> Option<f64> {
    match v {
        // Rounded to nearest: an `i64` is one Erlang digit at most.
        #[allow(clippy::cast_precision_loss)]
        Value::Int(n) => Some(*n as f64),
        Value::Big(b) => big_to_f64(b),
        Value::Float(f) => Some(*f),
        _ => None,
    }
}

/// An integral float as an integer value (`trunc`/`round`/`floor`/`ceil` of a float).
pub(crate) fn int_of_integral(f: f64) -> Result<Value, EvalError> {
    // 2^63: below it (and from -2^63) the float converts exactly.
    if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&f) {
        // Range-checked just above; `f` is integral.
        #[allow(clippy::cast_possible_truncation)]
        return Ok(Value::Int(f as i64));
    }
    from_big(BigInt::from_f64(f).ok_or_else(|| EvalError::new("not a finite number"))?)
}

/// An integer against a float, exactly — Erlang compares the two by value, so
/// `9007199254740993 > 9007199254740992.0` although both are the same `f64`.
pub(crate) fn cmp_int_float(n: &Value, f: f64) -> Ordering {
    let t = f.trunc();
    let by_int = match n {
        Value::Int(i)
            if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&t) =>
        {
            // Range-checked just above; `t` is integral.
            #[allow(clippy::cast_possible_truncation)]
            i.cmp(&(t as i64))
        }
        Value::Int(_) if t > 0.0 => Ordering::Less,
        Value::Int(_) => Ordering::Greater,
        _ => match (big(n), BigInt::from_f64(t)) {
            (Some(b), Some(t)) => b.as_ref().cmp(&t),
            _ => Ordering::Equal,
        },
    };
    // Equal integral parts: the float's fraction decides.
    by_int.then_with(|| {
        if f > t {
            Ordering::Less
        } else if f < t {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    })
}

/// `a + b`, `a - b`, `a * b`, `a div b`, `a rem b` on integers of any size.
pub(crate) fn int_arith(op: IntOp, a: &Value, b: &Value) -> Result<Value, EvalError> {
    if let (Value::Int(x), Value::Int(y)) = (a, b) {
        let fast = match op {
            IntOp::Add => x.checked_add(*y),
            IntOp::Sub => x.checked_sub(*y),
            IntOp::Mul => x.checked_mul(*y),
            // Erlang `div` truncates toward zero and `rem` takes the dividend's sign —
            // Rust's `/` and `%` on integers. Only `i64::MIN div -1` overflows.
            IntOp::Div | IntOp::Rem if *y == 0 => return Err(EvalError::new("division by zero")),
            IntOp::Div => x.checked_div(*y),
            IntOp::Rem => x.checked_rem(*y),
        };
        if let Some(n) = fast {
            return Ok(Value::Int(n));
        }
    }
    let (Some(x), Some(y)) = (big(a), big(b)) else {
        return Err(EvalError::new("expected integers"));
    };
    let (x, y) = (x.as_ref(), y.as_ref());
    // Refuse before computing: a product's size is the sum of its factors'.
    if op == IntOp::Mul && x.bits() + y.bits() > MAX_INT_BITS + 1 {
        return Err(too_large());
    }
    from_big(match op {
        IntOp::Add => x + y,
        IntOp::Sub => x - y,
        IntOp::Mul => x * y,
        IntOp::Div | IntOp::Rem if y.is_zero() => return Err(EvalError::new("division by zero")),
        IntOp::Div => x / y,
        IntOp::Rem => x % y,
    })
}

/// An integer operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

/// `-n` on an integer.
pub(crate) fn int_neg(v: &Value) -> Result<Value, EvalError> {
    match v {
        Value::Int(n) => match n.checked_neg() {
            Some(m) => Ok(Value::Int(m)),
            None => from_big(-BigInt::from(*n)),
        },
        Value::Big(b) => from_big(-b.as_ref()),
        _ => Err(EvalError::new("expected an integer")),
    }
}

/// `abs/1` on an integer.
pub(crate) fn int_abs(v: &Value) -> Result<Value, EvalError> {
    match v {
        Value::Int(n) if *n >= 0 => Ok(Value::Int(*n)),
        Value::Big(b) if !b.is_negative() => Ok(v.clone()),
        _ => int_neg(v),
    }
}

/// `X bsl S` (a negative `S` shifts right): the arithmetic shift Erlang's `bsl` and
/// `bsr` are, refused when the result would pass [`MAX_INT_BITS`].
pub(crate) fn int_shift_left(x: &Value, s: &Value) -> Result<Value, EvalError> {
    let Some(xb) = big(x) else {
        return Err(EvalError::new("expected an integer"));
    };
    if xb.is_zero() {
        return Ok(Value::Int(0));
    }
    let Some(s) = big(s) else {
        return Err(EvalError::new("expected an integer shift"));
    };
    let bits = xb.bits();
    match s.to_i64() {
        Some(s) if s >= 0 => {
            let s = u64::try_from(s).unwrap_or(u64::MAX);
            if bits.saturating_add(s) > MAX_INT_BITS + 1 {
                return Err(too_large());
            }
            if let Value::Int(n) = x {
                if s < 63 {
                    let r = n << s;
                    if r >> s == *n {
                        return Ok(Value::Int(r));
                    }
                }
            }
            from_big(xb.as_ref() << usize::try_from(s).map_err(|_| too_large())?)
        }
        // Right: past the integer's width every bit is its sign.
        Some(s) => {
            let s = s.unsigned_abs();
            if s >= bits {
                return Ok(Value::Int(if xb.is_negative() { -1 } else { 0 }));
            }
            if let Value::Int(n) = x {
                return Ok(Value::Int(n >> s));
            }
            from_big(xb.as_ref() >> usize::try_from(s).unwrap_or(usize::MAX))
        }
        None if s.is_negative() => Ok(Value::Int(if xb.is_negative() { -1 } else { 0 })),
        None => Err(too_large()),
    }
}

/// A bitwise operation on two integers (two's complement of unbounded width).
pub(crate) fn int_bitwise(op: BitOp, a: &Value, b: &Value) -> Result<Value, EvalError> {
    if let (Value::Int(x), Value::Int(y)) = (a, b) {
        return Ok(Value::Int(match op {
            BitOp::And => x & y,
            BitOp::Or => x | y,
            BitOp::Xor => x ^ y,
        }));
    }
    let (Some(x), Some(y)) = (big(a), big(b)) else {
        return Err(EvalError::new("expected integers"));
    };
    let (x, y) = (x.as_ref(), y.as_ref());
    from_big(match op {
        BitOp::And => x & y,
        BitOp::Or => x | y,
        BitOp::Xor => x ^ y,
    })
}

/// A bitwise operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BitOp {
    And,
    Or,
    Xor,
}

/// `bnot X`.
pub(crate) fn int_not(v: &Value) -> Result<Value, EvalError> {
    match v {
        Value::Int(n) => Ok(Value::Int(!n)),
        Value::Big(b) => from_big(!b.as_ref()),
        _ => Err(EvalError::new("expected an integer")),
    }
}

/// `binary_to_integer/1`: an optional sign and decimal digits, nothing else (no
/// whitespace, no `_`). `Ok(None)` when the text is not such an integer.
pub(crate) fn parse_int(s: &[u8]) -> Result<Option<Value>, EvalError> {
    let digits = match s.first() {
        Some(b'+' | b'-') => &s[1..],
        _ => s,
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Ok(None);
    }
    let text = std::str::from_utf8(s).unwrap_or_default();
    if let Ok(n) = text.parse::<i64>() {
        return Ok(Some(Value::Int(n)));
    }
    if digits.len() - digits.iter().take_while(|d| **d == b'0').count() > MAX_INT_DIGITS {
        return Err(too_large());
    }
    let n: BigInt = text
        .parse()
        .map_err(|_| EvalError::new("malformed integer"))?;
    from_big(n).map(Some)
}

/// `binary_to_float/1`: `[+-]digits.digits`, optionally `e[+-]digits` — Erlang's float
/// syntax, so `1e5`, `.5` and `5.` are not floats. `None` when the text is not one or
/// the float would not be finite; a float too small for a double is a (signed) zero.
pub(crate) fn parse_float(s: &[u8]) -> Option<f64> {
    let mut i = usize::from(matches!(s.first(), Some(b'+' | b'-')));
    let digits = |i: &mut usize| {
        let start = *i;
        while s.get(*i).is_some_and(u8::is_ascii_digit) {
            *i += 1;
        }
        *i > start
    };
    if !digits(&mut i) || s.get(i) != Some(&b'.') {
        return None;
    }
    i += 1;
    if !digits(&mut i) {
        return None;
    }
    if matches!(s.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(s.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        if !digits(&mut i) {
            return None;
        }
    }
    if i != s.len() {
        return None;
    }
    std::str::from_utf8(s)
        .ok()?
        .parse::<f64>()
        .ok()
        .filter(|f| f.is_finite())
}

/// Erlang's shortest float text — `float_to_binary(F, [short])`, which is also how jiffy
/// writes a float into JSON: the fewest digits that read back as `x` (Ryu's, the
/// algorithm both use), in fixed notation (`100.0`, `0.0001`, `123456789012.5`) or
/// scientific (`1.0e3`, `1.0e-5`, `1.2345678901234567e19`), whichever is shorter, fixed
/// on a tie, and scientific from 2^53 up. A `-0.0` keeps its sign here; JSON drops it.
#[must_use]
pub(crate) fn short_float(x: f64) -> String {
    let (digits, exp) = shortest_digits(x.abs());
    let n = i64::try_from(digits.len()).unwrap_or(1);
    let sci = format!(
        "{}.{}e{exp}",
        &digits[..1],
        if digits.len() > 1 { &digits[1..] } else { "0" }
    );
    let dec = if exp >= n - 1 {
        let zeros = usize::try_from(exp - (n - 1)).unwrap_or(0);
        format!("{digits}{}.0", "0".repeat(zeros))
    } else if exp >= 0 {
        let at = usize::try_from(exp + 1).unwrap_or(1);
        format!("{}.{}", &digits[..at], &digits[at..])
    } else {
        let zeros = usize::try_from(-exp - 1).unwrap_or(0);
        format!("0.{}{digits}", "0".repeat(zeros))
    };
    // 2^53: from here on, Erlang always writes scientific notation.
    let body = if x.abs() >= 9_007_199_254_740_992.0 || sci.len() < dec.len() {
        sci
    } else {
        dec
    };
    let sign = if x.is_sign_negative() { "-" } else { "" };
    format!("{sign}{body}")
}

/// The shortest digits that read back as `v` (finite, not negative) and the decimal
/// exponent of the first. Rust's shortest form is Ryu's but for one case: when `v` lies
/// exactly halfway between two shortest candidates, Rust takes the upper and Ryu the
/// even one (`967562026147900.25` is `967562026147900.2` in EMQX).
fn shortest_digits(v: f64) -> (String, i64) {
    let s = format!("{v:e}");
    let (mant, exp) = s.split_once('e').unwrap_or((&s, "0"));
    let mut digits: String = mant.chars().filter(char::is_ascii_digit).collect();
    let exp: i64 = exp.parse().unwrap_or(0);
    let odd = digits.bytes().last().is_some_and(|d| (d - b'0') % 2 == 1);
    if !odd || digits.len() > 18 {
        return (digits, exp);
    }
    let n = i64::try_from(digits.len()).unwrap_or(1);
    // `v` is `D * 10^last` rounded; a tie is `v = (D ± 1/2) * 10^last` exactly.
    let last = exp - (n - 1);
    let d: u128 = digits.parse().unwrap_or(0);
    for down in [true, false] {
        let twice = if down { 2 * d - 1 } else { 2 * d + 1 };
        if !is_exactly(v, twice * 5, last - 1) {
            continue;
        }
        let alt = if down { d - 1 } else { d + 1 };
        let mut alt_digits = alt.to_string();
        // A carry (`…9 + 1`) adds a digit: the exponent of the first moves up.
        let alt_exp = exp + i64::try_from(alt_digits.len()).unwrap_or(0) - n;
        while alt_digits.len() > 1 && alt_digits.ends_with('0') {
            alt_digits.pop();
        }
        let reads_back = format!("{}.{}e{alt_exp}", &alt_digits[..1], &alt_digits[1..])
            .parse::<f64>()
            .is_ok_and(|r| r.to_bits() == v.to_bits());
        if reads_back {
            digits = alt_digits;
            return (digits, alt_exp);
        }
    }
    (digits, exp)
}

/// Whether `v` (finite, not negative) is exactly `m * 10^e`.
fn is_exactly(v: f64, m: u128, e: i64) -> bool {
    if e >= 0 {
        // An integral `v`: its exact digits are `m` followed by `e` zeros.
        if v.fract() != 0.0 {
            return false;
        }
        let exact = format!("{v:.0}");
        let zeros = usize::try_from(e).unwrap_or(usize::MAX);
        let m = m.to_string();
        exact.len() == m.len().saturating_add(zeros)
            && exact.starts_with(&m)
            && exact[m.len()..].bytes().all(|c| c == b'0')
    } else {
        // `v` must have exactly `-e` decimals (then printing that many is exact).
        let want = usize::try_from(-e).unwrap_or(usize::MAX);
        if decimals_of(v) != Some(want) {
            return false;
        }
        let exact: String = format!("{v:.want$}")
            .chars()
            .filter(char::is_ascii_digit)
            .collect();
        exact.trim_start_matches('0') == m.to_string()
    }
}

/// How many decimals `v` has written out exactly (`None` for zero or a non-finite).
fn decimals_of(v: f64) -> Option<usize> {
    if v == 0.0 || !v.is_finite() {
        return None;
    }
    let bits = v.to_bits();
    let biased = i64::try_from((bits >> 52) & 0x7ff).unwrap_or(0);
    let frac = bits & ((1 << 52) - 1);
    let (mantissa, e) = if biased == 0 {
        (frac, -1074)
    } else {
        (frac | (1 << 52), biased - 1075)
    };
    // v = mantissa * 2^e; with the mantissa odd, v has -e decimals when e < 0.
    let e = e + i64::from(mantissa.trailing_zeros());
    Some(usize::try_from(-e).unwrap_or(0))
}

/// The decimals `float_to_binary(F, [{decimals, D}])` accepts.
pub(crate) const MAX_DECIMALS: i64 = 253;

/// Erlang's `float_to_binary(F, [{decimals, D}])`, with `compact` when asked — the Erlang
/// VM's own algorithm (`sys_double_to_chars_fast`), quirks included, since EMQX's `str`,
/// templates and `float2str` print with it:
///
/// - Up to 2^53 and with fewer than 19 decimals, the fraction is scaled by `10^D` *in
///   floating point* and rounded half away from zero, so `0.125` to two decimals is
///   `0.13`, and `0.995` (just below 0.995 exactly) is `1.00`. `compact` drops trailing
///   zeros but one, and only when `D > 0`.
/// - Otherwise the exact value is printed (`%.*f`, a tie to even), and `compact` drops
///   trailing zeros even with no decimal point: `float2str(1.0e20, 0)` is `"1"` in EMQX.
///   A text longer than 255 bytes is refused — `str(1.0e250)` fails in EMQX.
///
/// The sign is the float's sign bit: `-0.0` and `-1.0e-11` print as `-0.0`.
pub(crate) fn float_to_decimals(f: f64, decimals: i64, compact: bool) -> Result<String, EvalError> {
    const POW10: [f64; 19] = [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
        1e17, 1e18,
    ];
    let d = usize::try_from(decimals)
        .ok()
        .filter(|_| decimals <= MAX_DECIMALS)
        .ok_or_else(|| EvalError::new(format!("decimals must be in 0..={MAX_DECIMALS}")))?;
    let af = f.abs();
    if af > 9_007_199_254_740_992.0 || d >= POW10.len() {
        // Every finite double's integer part has at most 309 digits; refuse before
        // formatting what Erlang's 256-byte buffer would refuse.
        if af >= 1.0e255 {
            return Err(too_long_float(f, decimals));
        }
        let mut s = format!("{f:.d$}");
        if s.len() > 255 {
            return Err(too_long_float(f, decimals));
        }
        if compact {
            trim_trailing_zeros(&mut s);
        }
        return Ok(s);
    }
    let mut s = String::with_capacity(24 + d);
    if f.is_sign_negative() {
        s.push('-');
    }
    // `af <= 2^53` here, so these conversions are exact.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    if d == 0 {
        let _ = write!(s, "{}", af.round() as u64);
    } else {
        let int_f = af.floor();
        let frac_f = ((af - int_f) * POW10[d]).round();
        let (int_part, frac_part) = if frac_f >= POW10[d] {
            (int_f as u64 + 1, 0)
        } else {
            (int_f as u64, frac_f as u64)
        };
        let _ = write!(s, "{int_part}.{frac_part:0d$}");
        if compact {
            trim_trailing_zeros(&mut s);
        }
    }
    Ok(s)
}

/// erts's `find_first_trailing_zero`: drop trailing `0`s, but keep one after a `.`.
fn trim_trailing_zeros(s: &mut String) {
    while s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.push('0');
    }
}

fn too_long_float(f: f64, decimals: i64) -> EvalError {
    EvalError::new(format!(
        "{} with {decimals} decimals is longer than 255 characters, which Erlang's \
         float_to_binary refuses",
        short_float(f)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_round_as_the_erlang_vm_does() {
        let d = |f, n| float_to_decimals(f, n, true).unwrap();
        assert_eq!(d(0.125, 2), "0.13");
        assert_eq!(d(-0.125, 2), "-0.13");
        assert_eq!(d(2.5, 0), "3");
        assert_eq!(float_to_decimals(9.5, 0, false).unwrap(), "10");
        assert_eq!(d(0.995, 2), "1.0");
        assert_eq!(d(0.000_488_281_25, 10), "0.0004882813");
        assert_eq!(d(-0.0, 3), "-0.0");
        assert_eq!(d(1.0e20, 0), "1");
        assert_eq!(d(1.0e20, 2), "100000000000000000000.0");
        assert!(float_to_decimals(2.0e243, 11, true).is_err());
        assert_eq!(d(2.0e243, 10).len(), 246);
    }
}
