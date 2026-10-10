//! Erlang's `io_lib:format/2`, which EMQX's `sprintf(Format, ...)` calls as
//! `iolist_to_binary(io_lib:format(binary_to_list(Format), Args))`.
//!
//! Follows OTP's `io_lib_format.erl`, `io_lib.erl` and `io_lib_pretty.erl` (maint-28),
//! with every rule value read as the Erlang term EMQX holds for it: a string is a
//! binary, an array a list, a map a map with binary keys, `true`/`false`/`null` atoms
//! and a missing value the atom `undefined`. Two consequences shape the output:
//!
//! - The format is `binary_to_list`ed, so it is read as BYTES: UTF-8 text in it passes
//!   through unchanged, byte by byte.
//! - The result goes through `iolist_to_binary`, so every output character must fit in
//!   a byte. `~ts` and `~tc` produce Unicode characters; one above U+00FF fails the call,
//!   and one in U+0080..U+00FF comes out as its Latin-1 byte.
//!
//! One part is not reproduced: `~p` and `~P` print a term on one line, where
//! `io_lib_pretty` breaks a list, map or non-printable binary across lines once its
//! single-line text reaches the line width (80 columns by default, or the field width;
//! `~0p` never breaks).

use num_traits::ToPrimitive as _;

use crate::value::Value;
use crate::EvalError;

use super::erl_string;

/// One output "character": a code point, which must fit in a byte by the end.
type Chars = Vec<u32>;

fn err(msg: impl Into<String>) -> EvalError {
    EvalError::new(msg.into())
}

/// The fields of one control sequence, `~F.P.PadModC`.
struct Spec<'a> {
    width: Option<usize>,
    left: bool,
    prec: Option<i64>,
    pad: Chars,
    unicode: bool,
    strings: bool,
    ctrl: u8,
    args: Vec<&'a Value>,
}

/// Format `fmt` with `args`, at most `limit` characters long.
pub(crate) fn format(fmt: &[u8], args: &[Value], limit: usize) -> Result<Vec<u8>, EvalError> {
    let mut out: Chars = Vec::new();
    let mut args = args.iter();
    let mut i = 0;
    while i < fmt.len() {
        let c = fmt[i];
        i += 1;
        if c == b'~' {
            let spec = parse_spec(fmt, &mut i, &mut args)?;
            control(&spec, limit, &mut out)?;
        } else {
            out.push(u32::from(c));
        }
        if out.len() > limit {
            return Err(err("the output is too large"));
        }
    }
    if args.next().is_some() {
        return Err(err("more arguments than control sequences"));
    }
    out.into_iter()
        .map(|c| u8::try_from(c).map_err(|_| err("a character above U+00FF cannot be output")))
        .collect()
}

fn next_arg<'a>(args: &mut std::slice::Iter<'a, Value>) -> Result<&'a Value, EvalError> {
    args.next()
        .ok_or_else(|| err("more control sequences than arguments"))
}

/// `field_value/2`: `*` (an integer argument) or digits; `None` when neither.
fn field_value(
    fmt: &[u8],
    i: &mut usize,
    args: &mut std::slice::Iter<'_, Value>,
) -> Result<Option<i64>, EvalError> {
    match fmt.get(*i) {
        Some(b'*') => {
            // `field_value([$*|Fmt], [A|Args]) when is_integer(A)`; otherwise the `*` is
            // left in place and later read as an (unknown) control character.
            match args.as_slice().first() {
                Some(Value::Int(n)) => {
                    args.next();
                    *i += 1;
                    Ok(Some(*n))
                }
                // Wider than 64 bits: no field is that wide; the output limit refuses it.
                Some(Value::Big(n)) => {
                    args.next();
                    *i += 1;
                    Ok(Some(if n.sign() == num_bigint::Sign::Minus {
                        i64::MIN
                    } else {
                        i64::MAX
                    }))
                }
                _ => Ok(None),
            }
        }
        Some(d) if d.is_ascii_digit() => {
            let mut n: i64 = 0;
            while let Some(d) = fmt.get(*i).filter(|d| d.is_ascii_digit()) {
                n = n
                    .checked_mul(10)
                    .and_then(|n| n.checked_add(i64::from(d - b'0')))
                    .ok_or_else(|| err("field width out of range"))?;
                *i += 1;
            }
            Ok(Some(n))
        }
        _ => Ok(None),
    }
}

fn parse_spec<'a>(
    fmt: &[u8],
    i: &mut usize,
    args: &mut std::slice::Iter<'a, Value>,
) -> Result<Spec<'a>, EvalError> {
    let negated = fmt.get(*i) == Some(&b'-');
    if negated {
        *i += 1;
    }
    let (width, left) = match (negated, field_value(fmt, i, args)?) {
        // `-` with no width is `-none`: badarith.
        (true, None) => return Err(err("'-' without a field width")),
        (true, Some(f)) => (Some(-f), false),
        (false, f) => (f, false),
    };
    // A negative width means left-adjusted.
    let (width, left) = match width {
        Some(f) if f < 0 => (Some(f.unsigned_abs()), true),
        Some(f) => (Some(f.unsigned_abs()), left),
        None => (None, left),
    };
    let width = width
        .map(|w| usize::try_from(w).map_err(|_| err("field width out of range")))
        .transpose()?;
    let prec = if fmt.get(*i) == Some(&b'.') {
        *i += 1;
        field_value(fmt, i, args)?
    } else {
        None
    };
    let pad = match (fmt.get(*i), fmt.get(*i + 1)) {
        (Some(b'.'), Some(b'*')) if !args.as_slice().is_empty() => {
            *i += 2;
            pad_chars(next_arg(args)?)?
        }
        (Some(b'.'), Some(&p)) => {
            *i += 2;
            vec![u32::from(p)]
        }
        _ => vec![u32::from(b' ')],
    };
    let (mut unicode, mut strings) = (false, true);
    loop {
        match fmt.get(*i) {
            Some(b't') => unicode = true,
            Some(b'l') => strings = false,
            // `k` orders maps by key, which is how this always prints them.
            Some(b'k') => {}
            // `K` takes the order as an argument; no rule value is one.
            Some(b'K') => return Err(err("~K takes a map ordering, which a rule cannot give")),
            _ => break,
        }
        *i += 1;
    }
    let ctrl = *fmt
        .get(*i)
        .ok_or_else(|| err("the format ends inside a control sequence"))?;
    *i += 1;
    let n = match ctrl {
        b'~' | b'n' => 0,
        b'W' | b'P' | b'x' | b'X' => 2,
        b'w' | b'p' | b's' | b'e' | b'f' | b'g' | b'b' | b'B' | b'+' | b'#' | b'c' | b'i' => 1,
        other => {
            return Err(err(format!(
                "unknown control sequence ~{}",
                char::from(other).escape_default()
            )))
        }
    };
    let args = (0..n).map(|_| next_arg(args)).collect::<Result<_, _>>()?;
    Ok(Spec {
        width,
        left,
        prec,
        pad,
        unicode,
        strings,
        ctrl,
        args,
    })
}

/// A padding term from `.*`: a character code, or a binary's bytes.
fn pad_chars(v: &Value) -> Result<Chars, EvalError> {
    match v {
        Value::Int(c) => Ok(vec![
            u32::try_from(*c).map_err(|_| err("bad padding character"))?
        ]),
        v => v
            .as_bytes()
            .map(|b| b.iter().map(|c| u32::from(*c)).collect())
            .ok_or_else(|| err("bad padding character")),
    }
}

fn prec_usize(p: i64) -> Result<usize, EvalError> {
    usize::try_from(p).map_err(|_| err("negative precision"))
}

/// `chars(C, N)`, refused past the output limit (a field width can come from the payload).
fn repeat(unit: &[u32], n: usize, limit: usize) -> Result<Chars, EvalError> {
    if n.saturating_mul(unit.len()) > limit {
        return Err(err("the output is too large"));
    }
    Ok(unit.repeat(n))
}

/// `adjust(Data, Pad, Adj)`.
fn adjust(mut data: Chars, pad: Chars, left: bool) -> Chars {
    if left {
        data.extend(pad);
        data
    } else {
        let mut p = pad;
        p.extend(data);
        p
    }
}

// `f`, `p`, `s`, `n`, `d`: the field width, precision, spec, number and depth, named
// as the OTP source this follows names them.
#[allow(clippy::many_single_char_names, clippy::too_many_lines)]
fn control(s: &Spec, limit: usize, out: &mut Chars) -> Result<(), EvalError> {
    let (f, p) = (s.width, s.prec);
    let piece = match s.ctrl {
        b's' => {
            let text = string_arg(s.args[0], s.unicode)?;
            string(text, f, s.left, p, &s.pad, s.unicode, limit)?
        }
        b'w' => {
            let mut t = Chars::new();
            write_term(s.args[0], -1, &mut t);
            term(t, f, s.left, p, &s.pad, limit)?
        }
        b'W' => {
            let mut t = Chars::new();
            write_term(s.args[0], depth(s.args[1])?, &mut t);
            term(t, f, s.left, p, &s.pad, limit)?
        }
        b'p' | b'P' => {
            if s.left {
                return Err(err("~p cannot be left-adjusted"));
            }
            let d = if s.ctrl == b'P' {
                depth(s.args[1])?
            } else {
                -1
            };
            let mut t = Chars::new();
            if d == 0 {
                push_str(&mut t, "...");
            } else {
                pretty(s.args[0], d, s.strings, s.unicode, &mut t);
            }
            t
        }
        b'e' | b'f' | b'g' => {
            let Value::Float(x) = s.args[0] else {
                return Err(err(format!("~{} takes a float", char::from(s.ctrl))));
            };
            match s.ctrl {
                b'e' => fwrite_e(*x, f, s.left, p, &s.pad, limit)?,
                b'f' => fwrite_f(*x, f, s.left, p, &s.pad, limit)?,
                _ => fwrite_g(*x, f, s.left, p, &s.pad, limit)?,
            }
        }
        b'b' | b'B' | b'x' | b'X' | b'+' | b'#' => {
            let Some(n) = crate::num::big(s.args[0]) else {
                return Err(err(format!("~{} takes an integer", char::from(s.ctrl))));
            };
            let base = match p {
                None => 10,
                Some(b) if (2..=36).contains(&b) => u32::try_from(b).unwrap_or(10),
                Some(_) => return Err(err("an integer base must be 2..36")),
            };
            let lower = matches!(s.ctrl, b'b' | b'x' | b'+');
            let prefix: Chars = match s.ctrl {
                b'x' | b'X' => prefix_chars(s.args[1])?,
                b'+' | b'#' => format!("{base}#").bytes().map(u32::from).collect(),
                _ => Chars::new(),
            };
            let mut t = Chars::new();
            if n.sign() == num_bigint::Sign::Minus {
                t.push(u32::from(b'-'));
            }
            t.extend(prefix);
            let digits = n.magnitude().to_str_radix(base);
            push_str(
                &mut t,
                &if lower {
                    digits
                } else {
                    digits.to_ascii_uppercase()
                },
            );
            term(t, f, s.left, None, &s.pad, limit)?
        }
        b'c' => {
            let Some(n) = crate::num::big(s.args[0]) else {
                return Err(err("~c takes an integer"));
            };
            let c = if s.unicode {
                n.to_u32()
                    .ok_or_else(|| err("~tc takes a character code"))?
            } else {
                // `A band 255`, two's complement for a negative code (of any width).
                (n.as_ref() & num_bigint::BigInt::from(255))
                    .to_u32()
                    .unwrap_or(0)
            };
            char_field(c, f, s.left, p, &s.pad, limit)?
        }
        b'~' => char_field(u32::from(b'~'), f, s.left, p, &s.pad, limit)?,
        b'n' => match (f, s.left) {
            (None, _) => vec![u32::from(b'\n')],
            (Some(n), false) => repeat(&[u32::from(b'\n')], n, limit)?,
            (Some(_), true) => return Err(err("~n cannot be left-adjusted")),
        },
        // `~i` ignores its argument.
        _ => Chars::new(),
    };
    out.extend(piece);
    Ok(())
}

fn depth(v: &Value) -> Result<i64, EvalError> {
    match v {
        Value::Int(d) => Ok(*d),
        _ => Err(err("~W and ~P take an integer depth")),
    }
}

fn push_str(out: &mut Chars, s: &str) {
    out.extend(s.chars().map(u32::from));
}

/// The prefix of `~x`/`~X`: an atom's name or a (deep) character list; a binary is not
/// one (`io_lib:deep_char_list/1`).
fn prefix_chars(v: &Value) -> Result<Chars, EvalError> {
    fn deep(v: &Value, out: &mut Chars) -> bool {
        match v {
            Value::Int(c) => match u32::try_from(*c) {
                Ok(c) if char::from_u32(c).is_some() => {
                    out.push(c);
                    true
                }
                _ => false,
            },
            Value::Array(a) => a.iter().all(|x| deep(x, out)),
            _ => false,
        }
    }
    if let Some(name) = atom_name(v) {
        return Ok(name.bytes().map(u32::from).collect());
    }
    let mut out = Chars::new();
    if matches!(v, Value::Array(_)) && deep(v, &mut out) {
        Ok(out)
    } else {
        Err(err("the ~x prefix must be a character list"))
    }
}

fn atom_name(v: &Value) -> Option<&'static str> {
    match v {
        Value::Bool(true) => Some("true"),
        Value::Bool(false) => Some("false"),
        Value::Null => Some("null"),
        Value::Undefined => Some("undefined"),
        _ => None,
    }
}

/// The characters `~s` prints: an atom's name, or an iolist (`~s`: bytes and characters
/// up to 255) or chardata (`~ts`: any character; a binary is decoded as UTF-8, or kept
/// as bytes when it is not UTF-8). A number or a map is not text: `badarg`.
fn string_arg(v: &Value, unicode: bool) -> Result<Chars, EvalError> {
    fn walk(v: &Value, unicode: bool, out: &mut Chars) -> Result<(), EvalError> {
        match v {
            Value::Str(_) | Value::Bin(_) => {
                let b = v.as_bytes().unwrap_or_default();
                match std::str::from_utf8(b) {
                    Ok(s) if unicode => out.extend(s.chars().map(u32::from)),
                    _ => out.extend(b.iter().map(|c| u32::from(*c))),
                }
                Ok(())
            }
            Value::Int(c) => {
                let c = u32::try_from(*c).map_err(|_| err("~s takes text"))?;
                if !unicode && c > 255 {
                    return Err(err("~s takes text (characters up to 255)"));
                }
                out.push(c);
                Ok(())
            }
            Value::Array(a) => a.iter().try_for_each(|x| walk(x, unicode, out)),
            _ => Err(err("~s takes text")),
        }
    }
    if let Some(name) = atom_name(v) {
        return Ok(name.bytes().map(u32::from).collect());
    }
    if matches!(v, Value::Int(_)) {
        return Err(err("~s takes text, not a number"));
    }
    let mut out = Chars::new();
    walk(v, unicode, &mut out)?;
    Ok(out)
}

/// `io_lib:chars_length/1`: the number of characters when they all fit in a byte
/// (`iolist_size`), else the number of grapheme clusters.
fn chars_length(s: &[u32]) -> usize {
    if s.iter().all(|c| *c <= 255) {
        s.len()
    } else {
        erl_string::length(&to_string(s))
    }
}

fn to_string(s: &[u32]) -> String {
    s.iter().filter_map(|c| char::from_u32(*c)).collect()
}

/// `flat_trunc/3`: the first `n` characters (`~s`) or grapheme clusters (`~ts`).
fn flat_trunc(s: Chars, n: usize, unicode: bool) -> Chars {
    if unicode {
        let text = to_string(&s);
        erl_string::slice(&text, 0, Some(n))
            .chars()
            .map(u32::from)
            .collect()
    } else {
        s.into_iter().take(n).collect()
    }
}

/// `string/6`.
fn string(
    s: Chars,
    f: Option<usize>,
    left: bool,
    p: Option<i64>,
    pad: &[u32],
    unicode: bool,
    limit: usize,
) -> Result<Chars, EvalError> {
    let field = |s: Chars, f: usize, left: bool, n: usize| -> Result<Chars, EvalError> {
        Ok(match n.cmp(&f) {
            std::cmp::Ordering::Greater => flat_trunc(s, f, unicode),
            std::cmp::Ordering::Less => adjust(s, repeat(pad, f - n, limit)?, left),
            std::cmp::Ordering::Equal => s,
        })
    };
    let n = chars_length(&s);
    match (f, p) {
        (None, None) => Ok(s),
        (Some(f), None) => field(s, f, left, n),
        (None, Some(p)) => field(s, prec_usize(p)?, true, n),
        (Some(f), Some(p)) => {
            let p = prec_usize(p)?;
            if f < p {
                return Err(err("the precision is larger than the field width"));
            }
            if f == p {
                return field(s, f, left, n);
            }
            let body = match n.cmp(&p) {
                std::cmp::Ordering::Greater => flat_trunc(s, p, unicode),
                std::cmp::Ordering::Less => {
                    let mut s = s;
                    s.extend(repeat(pad, p - n, limit)?);
                    s
                }
                std::cmp::Ordering::Equal => s,
            };
            Ok(adjust(body, repeat(pad, f - p, limit)?, left))
        }
    }
}

/// `term/6`: a term's text in a field, all `*` when it does not fit.
fn term(
    t: Chars,
    f: Option<usize>,
    left: bool,
    p: Option<i64>,
    pad: &[u32],
    limit: usize,
) -> Result<Chars, EvalError> {
    let (f, p0) = match (f, p) {
        (None, None) => return Ok(t),
        (None, Some(p)) => {
            let p = prec_usize(p)?;
            (p, Some(p))
        }
        (Some(f), p) => (f, p.map(prec_usize).transpose()?),
    };
    let l = chars_length(&t);
    let p = l.min(p0.map_or(f, |p0| p0.min(f)));
    if l > p {
        Ok(adjust(
            repeat(&[u32::from(b'*')], p, limit)?,
            repeat(pad, f - p, limit)?,
            left,
        ))
    } else {
        Ok(adjust(t, repeat(pad, f - l, limit)?, left))
    }
}

/// `char/5`.
fn char_field(
    c: u32,
    f: Option<usize>,
    left: bool,
    p: Option<i64>,
    pad: &[u32],
    limit: usize,
) -> Result<Chars, EvalError> {
    match (f, p) {
        (None, None) => Ok(vec![c]),
        (Some(f), None) => repeat(&[c], f, limit),
        (None, Some(p)) => repeat(&[c], prec_usize(p)?, limit),
        (Some(f), Some(p)) => {
            let p = prec_usize(p)?;
            if f < p {
                return Err(err("the precision is larger than the field width"));
            }
            Ok(adjust(
                repeat(&[c], p, limit)?,
                repeat(pad, f - p, limit)?,
                left,
            ))
        }
    }
}

// -- floats

/// `float_data/1`: the 21 significant digits of `float_to_list(F)` (`%.20e`) and the
/// exponent plus one.
fn float_data(x: f64) -> (Vec<u8>, i64) {
    let s = format!("{:.20e}", x.abs());
    let (mant, exp) = s.split_once('e').unwrap_or((&s, "0"));
    let digits = mant.bytes().filter(u8::is_ascii_digit).collect();
    (digits, exp.parse::<i64>().unwrap_or(0) + 1)
}

/// `float_man/3`: `int` digits, a point and `dec` decimals from `ds`, rounded half up on
/// the next digit, and whether the rounding carried out of the first digit.
fn float_man(ds: &[u8], int: usize, dec: usize) -> (Vec<u8>, bool) {
    let n = int + dec;
    let mut d: Vec<u8> = (0..n).map(|k| ds.get(k).copied().unwrap_or(b'0')).collect();
    let mut carry = ds.get(n).is_some_and(|c| *c >= b'5');
    for k in (0..n).rev() {
        if !carry {
            break;
        }
        if d[k] == b'9' {
            d[k] = b'0';
        } else {
            d[k] += 1;
            carry = false;
        }
    }
    d.insert(int, b'.');
    (d, carry)
}

fn sign(x: f64) -> &'static str {
    if x.is_sign_negative() {
        "-"
    } else {
        ""
    }
}

fn float_exp(e: i64) -> String {
    if e >= 0 {
        format!("e+{e}")
    } else {
        format!("e{e}")
    }
}

/// `float_e/3`.
fn float_e(x: f64, p: usize) -> String {
    let (ds, e) = float_data(x);
    let (mut fs, carry) = float_man(&ds, 1, p - 1);
    let e = if carry {
        fs[0] = b'1';
        e
    } else {
        e - 1
    };
    format!(
        "{}{}{}",
        sign(x),
        String::from_utf8_lossy(&fs),
        float_exp(e)
    )
}

/// `float_f/3`.
fn float_f(x: f64, p: usize) -> String {
    let (mut ds, mut e) = float_data(x);
    if e <= 0 {
        let zeros = usize::try_from(-e + 1).unwrap_or(0);
        let mut z = vec![b'0'; zeros];
        z.extend(ds);
        ds = z;
        e = 1;
    }
    let (fs, carry) = float_man(&ds, usize::try_from(e).unwrap_or(1), p);
    let body = String::from_utf8_lossy(&fs);
    format!("{}{}{body}", sign(x), if carry { "1" } else { "" })
}

fn float_field(
    text: &str,
    f: Option<usize>,
    left: bool,
    pad: &[u32],
    limit: usize,
) -> Result<Chars, EvalError> {
    let t: Chars = text.chars().map(u32::from).collect();
    match f {
        None => Ok(t),
        Some(w) => term(t, Some(w), left, i64::try_from(w).ok(), pad, limit),
    }
}

fn float_prec(p: Option<i64>, min: i64, limit: usize) -> Result<usize, EvalError> {
    let p = p.unwrap_or(6);
    if p < min {
        return Err(err(format!("a float precision must be at least {min}")));
    }
    let p = prec_usize(p)?;
    if p > limit {
        return Err(err("the output is too large"));
    }
    Ok(p)
}

fn fwrite_e(
    x: f64,
    f: Option<usize>,
    left: bool,
    p: Option<i64>,
    pad: &[u32],
    limit: usize,
) -> Result<Chars, EvalError> {
    float_field(&float_e(x, float_prec(p, 2, limit)?), f, left, pad, limit)
}

fn fwrite_f(
    x: f64,
    f: Option<usize>,
    left: bool,
    p: Option<i64>,
    pad: &[u32],
    limit: usize,
) -> Result<Chars, EvalError> {
    float_field(&float_f(x, float_prec(p, 1, limit)?), f, left, pad, limit)
}

/// `fwrite_g/5`: `~f` with `P` significant digits for 0.1 <= |x| < 10000, else `~e`.
#[allow(clippy::many_single_char_names)]
fn fwrite_g(
    x: f64,
    f: Option<usize>,
    left: bool,
    p: Option<i64>,
    pad: &[u32],
    limit: usize,
) -> Result<Chars, EvalError> {
    let p = i64::try_from(float_prec(p, 1, limit)?).unwrap_or(6);
    let a = x.abs();
    // `None` stands for Erlang's `fwrite_f` atom, which compares above every integer.
    let e: Option<i64> = if a < 1.0e-1 {
        Some(-2)
    } else if a < 1.0e0 {
        Some(-1)
    } else if a < 1.0e1 {
        Some(0)
    } else if a < 1.0e2 {
        Some(1)
    } else if a < 1.0e3 {
        Some(2)
    } else if a < 1.0e4 {
        Some(3)
    } else {
        None
    };
    match e {
        Some(e) if (p <= 1 && e == -1) || (p - 1 > e && e >= -1) => {
            fwrite_f(x, f, left, Some(p - 1 - e), pad, limit)
        }
        _ if p <= 1 => fwrite_e(x, f, left, Some(2), pad, limit),
        _ => fwrite_e(x, f, left, Some(p), pad, limit),
    }
}

// -- terms

/// The keys of a map in Erlang's order for binary keys (byte-wise), which is the order
/// a map of up to 32 keys is stored, and printed, in.
fn sorted(m: &crate::value::Map) -> Vec<(&std::sync::Arc<str>, &Value)> {
    let mut entries: Vec<_> = m.iter().collect();
    entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    entries
}

/// `io_lib:write/2` (`~w`, `~W`) with depth `d` (-1: unlimited).
fn write_term(v: &Value, d: i64, out: &mut Chars) {
    if d == 0 {
        push_str(out, "...");
        return;
    }
    match v {
        Value::Int(n) => push_str(out, &n.to_string()),
        Value::Big(n) => push_str(out, &n.to_string()),
        Value::Float(x) => push_str(out, &crate::num::short_float(*x)),
        Value::Str(_) | Value::Bin(_) => write_binary(v.as_bytes().unwrap_or_default(), d, out),
        Value::Array(a) if a.is_empty() => push_str(out, "[]"),
        Value::Array(_) if d == 1 => push_str(out, "[...]"),
        Value::Array(a) => {
            push_str(out, "[");
            write_term(&a[0], d - 1, out);
            let mut d = d - 1;
            for x in &a[1..] {
                if d == 1 {
                    push_str(out, "|...");
                    break;
                }
                push_str(out, ",");
                write_term(x, d - 1, out);
                d -= 1;
            }
            push_str(out, "]");
        }
        Value::Map(m) if m.is_empty() || d == 1 => push_str(out, "#{}"),
        Value::Map(m) => {
            // write_map/4: the first pair, then write_map_body/5 — which writes `,...`
            // once its depth is 1, even when no pair is left.
            let d0 = d - 1;
            let mut entries = sorted(m).into_iter();
            let pair = |(k, x): (&std::sync::Arc<str>, &Value), out: &mut Chars| {
                write_binary(k.as_bytes(), d0, out);
                push_str(out, " => ");
                write_term(x, d0, out);
            };
            push_str(out, "#{");
            if let Some(first) = entries.next() {
                pair(first, out);
            }
            let mut body = d0;
            loop {
                if body == 1 {
                    push_str(out, ",...");
                    break;
                }
                let Some(next) = entries.next() else { break };
                push_str(out, ",");
                pair(next, out);
                body -= 1;
            }
            push_str(out, "}");
        }
        other => push_str(out, atom_name(other).unwrap_or("undefined")),
    }
}

/// `io_lib:write_binary/2`: `<<b,b,...>>`, `...` once the depth runs out.
fn write_binary(b: &[u8], d: i64, out: &mut Chars) {
    push_str(out, "<<");
    let mut d = d;
    for (n, byte) in b.iter().enumerate() {
        if d == 1 {
            push_str(out, "...");
            break;
        }
        push_str(out, &byte.to_string());
        if n + 1 < b.len() {
            push_str(out, ",");
        }
        d -= 1;
    }
    push_str(out, ">>");
}

/// `printable_char/2`.
fn printable(c: u32, unicode: bool) -> bool {
    matches!(c, 8..=13 | 27)
        || (0x20..=0x7e).contains(&c)
        || if unicode {
            (0xa0..0xd800).contains(&c)
                || (0xe000..0xfffe).contains(&c)
                || (0x1_0000..=0x10_ffff).contains(&c)
        } else {
            (0xa0..=0xff).contains(&c)
        }
}

/// `io_lib:write_string/2`: `"text"` with Erlang's escapes.
fn write_string(s: &[u32], out: &mut Chars) {
    push_str(out, "\"");
    for &c in s {
        match c {
            0x22 => push_str(out, "\\\""),
            0x5c => push_str(out, "\\\\"),
            0x0a => push_str(out, "\\n"),
            0x0d => push_str(out, "\\r"),
            0x09 => push_str(out, "\\t"),
            0x0b => push_str(out, "\\v"),
            0x08 => push_str(out, "\\b"),
            0x0c => push_str(out, "\\f"),
            0x1b => push_str(out, "\\e"),
            0x7f => push_str(out, "\\d"),
            c if c >= 0x20 && c != 0x7f && !(0x80..0xa0).contains(&c) => out.push(c),
            c => push_str(out, &format!("\\{}{}{}", (c >> 6) & 7, (c >> 3) & 7, c & 7)),
        }
    }
    push_str(out, "\"");
}

/// `io_lib_pretty`'s text for a term (`~p`, `~P`) with depth `d` (-1: unlimited), on one
/// line.
fn pretty(v: &Value, d: i64, strings: bool, unicode: bool, out: &mut Chars) {
    match v {
        Value::Str(_) | Value::Bin(_) => {
            pretty_binary(v.as_bytes().unwrap_or_default(), d, strings, unicode, out);
        }
        Value::Array(a) if a.is_empty() => push_str(out, "[]"),
        Value::Array(a) => {
            if strings && d != 1 {
                let chars: Option<Chars> = a
                    .iter()
                    .map(|x| match x {
                        Value::Int(c) => u32::try_from(*c).ok().filter(|c| printable(*c, unicode)),
                        _ => None,
                    })
                    .collect();
                if let Some(chars) = chars {
                    write_string(&chars, out);
                    return;
                }
            }
            push_str(out, "[");
            let mut d = d;
            for (n, x) in a.iter().enumerate() {
                if d == 1 {
                    push_str(out, if n == 0 { "..." } else { "|..." });
                    break;
                }
                if n > 0 {
                    push_str(out, ",");
                }
                pretty(x, d - 1, strings, unicode, out);
                d -= 1;
            }
            push_str(out, "]");
        }
        Value::Map(m) if m.is_empty() => push_str(out, "#{}"),
        Value::Map(_) if d == 1 => push_str(out, "#{...}"),
        Value::Map(m) => {
            let d0 = d - 1;
            push_str(out, "#{");
            let mut d = d;
            for (n, (k, x)) in sorted(m).into_iter().enumerate() {
                if n > 0 {
                    push_str(out, ",");
                }
                if d == 1 {
                    push_str(out, "...");
                    break;
                }
                pretty_binary(k.as_bytes(), d0, strings, unicode, out);
                push_str(out, " => ");
                pretty(x, d0, strings, unicode, out);
                d -= 1;
            }
            push_str(out, "}");
        }
        other => write_term(other, -1, out),
    }
}

/// `print_length_binary/7` for list output: printable text as `<<"...">>` (or
/// `<<"..."/utf8>>` for `~tp` with characters past ASCII), a printable prefix at a
/// limited depth, the bytes otherwise.
fn pretty_binary(b: &[u8], d: i64, strings: bool, unicode: bool, out: &mut Chars) {
    if b.is_empty() {
        push_str(out, "<<>>");
        return;
    }
    if d == 1 {
        push_str(out, "<<...>>");
        return;
    }
    let d1 = d - 1;
    let len = if d1 >= 0 {
        usize::try_from(4 * d1).unwrap_or(usize::MAX).min(b.len())
    } else {
        b.len()
    };
    if strings && len > 0 {
        if let Some((chars, whole, utf8)) = printable_prefix(b, len, d1, unicode) {
            push_str(out, "<<");
            write_string(&chars, out);
            if utf8 {
                push_str(out, "/utf8");
            }
            if !whole {
                push_str(out, "...");
            }
            push_str(out, ">>");
            return;
        }
    }
    write_binary(b, d, out);
}

/// `printable_bin/5`: the characters to print as a string, whether they are the whole
/// binary, and whether they are UTF-8 (`/utf8`).
fn printable_prefix(b: &[u8], len: usize, d: i64, unicode: bool) -> Option<(Chars, bool, bool)> {
    // printable_latin1_bin: the whole binary when every byte is printable; else, at a
    // limited depth, the printable prefix of the first `len` bytes when it is at least
    // `d` long.
    let latin1 = || {
        let n = b[..len]
            .iter()
            .take_while(|c| printable(u32::from(**c), false))
            .count();
        let chars = b[..n].iter().map(|c| u32::from(*c)).collect::<Chars>();
        if n == b.len() {
            Some((chars, true, false))
        } else {
            (d > 0 && i64::try_from(n).unwrap_or(0) >= d).then_some((chars, false, false))
        }
    };
    if !unicode {
        return latin1();
    }
    // printable_unicode_bin: up to `len` characters while printable.
    let mut chars = Chars::new();
    let mut at = 0;
    let mut left = len;
    while left > 0 && at < b.len() {
        let Some(c) = utf8_char(&b[at..]) else {
            return latin1();
        };
        if !printable(u32::from(c), true) {
            break;
        }
        chars.push(u32::from(c));
        at += c.len_utf8();
        left -= 1;
    }
    if at < b.len() && utf8_char(&b[at..]).is_none() {
        return latin1();
    }
    if at == b.len() {
        let ascii = chars.len() == b.len();
        return Some((chars, true, !ascii));
    }
    let n = i64::try_from(chars.len()).unwrap_or(0);
    (d > 0 && n >= d).then(|| {
        let ascii = chars.len() == at;
        (chars, false, !ascii)
    })
}

fn utf8_char(b: &[u8]) -> Option<char> {
    let n = match b.first()? {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    };
    std::str::from_utf8(b.get(..n)?).ok()?.chars().next()
}
