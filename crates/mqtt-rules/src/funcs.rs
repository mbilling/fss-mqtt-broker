//! The built-in SQL functions, by EMQX name and EMQX semantics.
//!
//! Every function here is named, typed and behaves as in EMQX's
//! `rule-sql-builtin-functions` reference — and where the reference and EMQX's source
//! (`emqx_rule_funcs.erl`, `emqx_variform_bif.erl`) differ, as the source runs; the
//! reference's examples are this module's tests, and probes of EMQX 6.3.1 pin the rest.
//! A call with an argument of the wrong type fails the rule's SQL (counted as a
//! failure), as EMQX's does.
//!
//! Deliberately absent (docs/RULES.md lists them): `jq`, schema registry and Sparkplug
//! functions, `maptab_lookup`, the `MongoDB` date helpers, the process-dictionary and
//! `kv_store_*` state functions (ADR 0085 designs rule state), and `term_encode` /
//! `term_decode`.

mod compress;
mod erl_string;
mod io_format;
pub(crate) mod re;

use std::sync::Arc;

use base64::Engine as _;

use self::re::{Groups, Regex};

use crate::eval::{resolve_index, EvalCtx};
use crate::value::{json_decode, Map, Value};
use crate::EvalError;

/// What a function sees beyond its arguments.
pub(crate) struct FnCtx<'a> {
    pub ctx: &'a EvalCtx<'a>,
    /// The load-time-compiled pattern (or why it does not compile) when the function's
    /// regex argument is a literal.
    pub regex: Option<&'a Compiled>,
}

/// A built-in function.
pub(crate) struct Func {
    pub name: &'static str,
    pub min: usize,
    pub max: usize,
    pub f: fn(&[Value], &FnCtx) -> Result<Value, EvalError>,
    /// Which argument is a regular expression (compiled once when it is a literal).
    pub regex_arg: Option<usize>,
}

impl std::fmt::Debug for Func {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name)
    }
}

const MANY: usize = usize::MAX;

/// How far one message's functions may grow strings beyond their inputs, TOGETHER —
/// every call, in every rule, in every `FOREACH` output: `pad`'s length, a
/// `replace`/`regex_replace` that substitutes a longer string at every match, a
/// `join_to_string` separator repeated between items. Sizes and strings like these are
/// often payload fields, and output that grows with the *product* of two of them lets
/// one small publish ask for gigabytes — an allocation failure aborts the process, as
/// does `str::repeat`'s capacity panic under `panic = "abort"`. A per-call bound alone
/// is not enough: a `FOREACH` repeats the call per element. Past the budget the
/// function fails, and so does the rule; the message is still routed.
pub(crate) const MAX_BUILT_BYTES: usize = 1 << 20;

/// The longest key path `map_put` / `mput` build. Each segment is one level of a nested
/// map, built recursively and later rendered and dropped recursively, and the path is
/// often a payload field.
const MAX_PATH_SEGMENTS: usize = 64;

/// Charge an output of `output` bytes (`None`: too large to count) built from inputs
/// totalling `input` bytes to the message's [`MAX_BUILT_BYTES`] budget, refusing it
/// when the budget would be exceeded.
fn bounded_growth(
    cx: &FnCtx,
    name: &str,
    input: usize,
    output: Option<usize>,
) -> Result<(), EvalError> {
    let built = output
        .map(|n| n.saturating_sub(input))
        .and_then(|g| g.checked_add(cx.ctx.built.get()))
        .filter(|&total| total <= MAX_BUILT_BYTES)
        .ok_or_else(|| {
            EvalError::new(format!(
                "{name} would build more than {MAX_BUILT_BYTES} bytes beyond its inputs \
                 (the budget is per message, across every function call)"
            ))
        })?;
    cx.ctx.built.set(built);
    Ok(())
}

/// The decimals `float()` and `float2str()` accept: Erlang's `float_to_binary`'s own
/// range, which is what EMQX formats with.
const MAX_DECIMALS: i64 = 253;

macro_rules! funcs {
    ($( $name:literal $min:literal ..= $max:tt => $f:expr $(, regex $r:literal)? ;)*) => {
        static FUNCS: &[Func] = &[
            $( Func {
                name: $name,
                min: $min,
                max: funcs!(@max $max),
                f: $f,
                regex_arg: funcs!(@re $($r)?),
            }, )*
        ];
    };
    (@max MANY) => { MANY };
    (@max $n:literal) => { $n };
    (@re) => { None };
    (@re $r:literal) => { Some($r) };
}

funcs! {
    // -- mathematical
    "abs" 1..=1 => |a, _| match &a[0] {
        Value::Int(n) => n.checked_abs().map(Value::Int).ok_or_else(overflow),
        v => Value::float(num(v)?.abs()),
    };
    "acos" 1..=1 => |a, _| math(a, f64::acos);
    "acosh" 1..=1 => |a, _| math(a, f64::acosh);
    "asin" 1..=1 => |a, _| math(a, f64::asin);
    "asinh" 1..=1 => |a, _| math(a, f64::asinh);
    "atan" 1..=1 => |a, _| math(a, f64::atan);
    "atanh" 1..=1 => |a, _| math(a, f64::atanh);
    "ceil" 1..=1 => |a, _| to_int_value(num(&a[0])?.ceil());
    "cos" 1..=1 => |a, _| math(a, f64::cos);
    "cosh" 1..=1 => |a, _| math(a, f64::cosh);
    "exp" 1..=1 => |a, _| math(a, f64::exp);
    "floor" 1..=1 => |a, _| to_int_value(num(&a[0])?.floor());
    "fmod" 2..=2 => |a, _| Value::float(num(&a[0])? % num(&a[1])?);
    "log" 1..=1 => |a, _| math(a, f64::ln);
    "log10" 1..=1 => |a, _| math(a, f64::log10);
    "log2" 1..=1 => |a, _| math(a, f64::log2);
    "round" 1..=1 => |a, _| to_int_value(num(&a[0])?.round());
    "power" 2..=2 => |a, _| Value::float(num(&a[0])?.powf(num(&a[1])?));
    "random" 0..=0 => |_, _| {
        let mut b = [0u8; 8];
        random_bytes(&mut b)?;
        // 53 random bits → [0, 1).
        #[allow(clippy::cast_precision_loss)]
        Value::float((u64::from_le_bytes(b) >> 11) as f64 / (1u64 << 53) as f64)
    };
    "sin" 1..=1 => |a, _| math(a, f64::sin);
    "sinh" 1..=1 => |a, _| math(a, f64::sinh);
    "sqrt" 1..=1 => |a, _| math(a, f64::sqrt);
    "tan" 1..=1 => |a, _| math(a, f64::tan);
    "tanh" 1..=1 => |a, _| math(a, f64::tanh);

    // -- data type judgment
    "is_array" 1..=1 => |a, _| Ok(Value::Bool(matches!(a[0], Value::Array(_))));
    "is_bool" 1..=1 => |a, _| Ok(Value::Bool(matches!(a[0], Value::Bool(_))));
    "is_float" 1..=1 => |a, _| Ok(Value::Bool(matches!(a[0], Value::Float(_))));
    "is_int" 1..=1 => |a, _| Ok(Value::Bool(matches!(a[0], Value::Int(_))));
    "is_map" 1..=1 => |a, _| Ok(Value::Bool(matches!(a[0], Value::Map(_))));
    "is_null" 1..=1 => |a, _| Ok(Value::Bool(a[0].is_undefined()));
    "is_not_null" 1..=1 => |a, _| Ok(Value::Bool(!a[0].is_undefined()));
    "is_null_var" 1..=1 => |a, _| Ok(Value::Bool(matches!(a[0], Value::Undefined | Value::Null)));
    "is_not_null_var" 1..=1 => |a, _| Ok(Value::Bool(!matches!(a[0], Value::Undefined | Value::Null)));
    "is_num" 1..=1 => |a, _| Ok(Value::Bool(a[0].is_number()));
    "is_str" 1..=1 => |a, _| Ok(Value::Bool(a[0].is_binary()));
    // EMQX: `[]` and `<<>>` are empty, any other list is not, and everything else goes
    // through `map/1` — so a non-empty string must be a JSON *object*; other JSON (an
    // array, `null`) or text that is not JSON fails the rule.
    "is_empty" 1..=1 => |a, _| Ok(Value::Bool(match &a[0] {
        Value::Array(x) => x.is_empty(),
        Value::Map(m) => m.is_empty(),
        v @ (Value::Str(_) | Value::Bin(_)) => {
            let b = v.as_bytes().unwrap_or_default();
            if b.is_empty() {
                true
            } else {
                match json_decode(b)? {
                    Value::Map(m) => m.is_empty(),
                    other => return Err(type_err("a map or a JSON object", &other)),
                }
            }
        }
        other => return Err(type_err("an array or a map", other)),
    }));

    // -- data type conversion
    "bool" 1..=1 => |a, _| match &a[0] {
        Value::Bool(b) => Ok(Value::Bool(*b)),
        v if v.is_number() && (num(v)? - 1.0).abs() < f64::EPSILON => Ok(Value::Bool(true)),
        v if v.is_number() && num(v)?.abs() < f64::EPSILON => Ok(Value::Bool(false)),
        v => match v.as_str() {
            Some("true") => Ok(Value::Bool(true)),
            Some("false") => Ok(Value::Bool(false)),
            _ => Err(EvalError::new(format!("cannot convert {} to a boolean", v.to_text()?))),
        },
    };
    "float" 1..=2 => |a, _| {
        let f = to_float(&a[0])?;
        match a.get(1) {
            None => Value::float(f),
            Some(d) => {
                let d = int(d)?;
                if !(1..=MAX_DECIMALS).contains(&d) {
                    return Err(EvalError::new(format!("decimals must be in 1..={MAX_DECIMALS}")));
                }
                let s = format!("{f:.prec$}", prec = usize::try_from(d).unwrap_or(1));
                Value::float(s.parse().map_err(|_| EvalError::new("float conversion failed"))?)
            }
        }
    };
    "float2str" 2..=2 => |a, _| {
        let f = to_float(&a[0])?;
        let d = int(&a[1])?;
        if !(0..=MAX_DECIMALS).contains(&d) {
            return Err(EvalError::new(format!("decimals must be in 0..={MAX_DECIMALS}")));
        }
        let d = usize::try_from(d).unwrap_or(0);
        Ok(Value::from(compact_decimals(&format!("{f:.d$}"))))
    };
    "int" 1..=1 => |a, _| Ok(Value::Int(to_int(&a[0])?));
    "str" 1..=1 => |a, _| Ok(Value::from(a[0].to_text()?));
    "str_utf8" 1..=1 => |a, _| Ok(Value::from(a[0].to_text()?));
    "str_utf16_le" 1..=1 => |a, _| {
        let s = a[0].to_text()?;
        Ok(Value::Bin(s.encode_utf16().flat_map(u16::to_le_bytes).collect::<Vec<u8>>().into()))
    };
    "map" 1..=1 => |a, _| match decode_if_text(&a[0]) {
        m @ Value::Map(_) => Ok(m),
        v => Err(type_err("a map or a JSON object", &v)),
    };

    // -- string operations
    // EMQX's `ascii(<<Char:8, _/binary>>) -> Char`: the first BYTE, not character.
    "ascii" 1..=1 => |a, _| s_bytes(&a[0])?
        .first()
        .map(|b| Value::Int(i64::from(*b)))
        .ok_or_else(|| EvalError::new("an empty string has no first byte"));
    "concat" 1..=MANY => |a, _| {
        let parts: Vec<Value> = match a {
            [Value::Array(list)] => list.as_ref().clone(),
            _ => a.to_vec(),
        };
        let mut s = String::new();
        for p in &parts {
            s.push_str(&p.to_text()?);
        }
        Ok(Value::from(s))
    };
    "find" 2..=3 => |a, _| {
        let (s, p) = (s_arg(&a[0])?, text(&a[1])?);
        let trailing = direction(a.get(2), &["leading", "trailing"])? == "trailing";
        Ok(Value::from(erl_string::find(s, p, trailing).unwrap_or("")))
    };
    "join_to_string" 1..=2 => |a, cx| {
        let (sep, list) = match a {
            [list] => (", ".to_string(), list),
            [sep, list] => (text(sep)?.to_string(), list),
            _ => unreachable!("arity checked at load"),
        };
        let items = array(list)?;
        let parts = items.iter().map(Value::to_text).collect::<Result<Vec<_>, _>>()?;
        let text_len: usize = parts.iter().map(String::len).sum();
        let seps = parts.len().saturating_sub(1).checked_mul(sep.len());
        bounded_growth(cx, "join_to_string", text_len + sep.len(), seps.and_then(|n| n.checked_add(text_len)))?;
        Ok(Value::from(parts.join(&sep)))
    };
    "lower" 1..=1 => |a, _| Ok(Value::from(erl_string::lowercase(s_arg(&a[0])?)));
    "ltrim" 1..=1 => |a, _| Ok(Value::from(erl_string::trim_leading(s_arg(&a[0])?, erl_string::WHITESPACE)));
    "pad" 2..=4 => |a, cx| {
        let s = s_arg(&a[0])?;
        let len = usize::try_from(int(&a[1])?).unwrap_or(0);
        let dir = direction(a.get(2), &["trailing", "leading", "both"])?;
        let ch = a.get(3).map(text).transpose()?.unwrap_or(" ");
        let missing = len.saturating_sub(erl_string::length(s));
        bounded_growth(cx, "pad", s.len(), missing.checked_mul(ch.len()).and_then(|n| n.checked_add(s.len())))?;
        let (left, right) = match dir {
            "leading" => (missing, 0),
            "both" => (missing / 2, missing - missing / 2),
            _ => (0, missing),
        };
        Ok(Value::from(format!("{}{s}{}", ch.repeat(left), ch.repeat(right))))
    };
    // `re:run(Str, RE, [global, {capture, none}])`: whether there is a first match.
    "regex_match" 2..=2 => |a, cx| {
        let s = s_bytes(&a[0])?;
        Ok(Value::Bool(regex(cx, &a[1])?.first(s).map_err(EvalError::new)?.is_some()))
    }, regex 1;
    // `re:replace(Str, RE, Rep, [global, {return, binary}])`.
    "regex_replace" 3..=3 => |a, cx| {
        let (s, rep) = (s_bytes(&a[0])?, bin(&a[2])?);
        let re = regex(cx, &a[1])?;
        bounded_replace_all(cx, &re, s, &erlang_replacement(rep)?)
    }, regex 1;
    // `re:run(Str, RE, [{capture, all_but_first, binary}])`: the first match's groups,
    // up to the last that took part, one that did not as `''`.
    "regex_extract" 2..=2 => |a, cx| {
        let s = s_bytes(&a[0])?;
        let groups = regex(cx, &a[1])?.first(s).map_err(EvalError::new)?.unwrap_or_default();
        Ok(Value::from(
            groups.iter().skip(1).map(|g| group_value(s, *g)).collect::<Vec<_>>(),
        ))
    }, regex 1;
    "replace" 3..=4 => |a, cx| {
        let (s, p, r) = (s_arg(&a[0])?, text(&a[1])?, text(&a[2])?);
        let at = match direction(a.get(3), &["all", "leading", "trailing"])? {
            "leading" => erl_string::Where::Leading,
            "trailing" => erl_string::Where::Trailing,
            _ => erl_string::Where::All,
        };
        // `lists:join(Replacement, string:split(S, Pattern, Where))`.
        let parts = erl_string::split(s, p, at);
        let kept: usize = parts.iter().map(|x| x.len()).sum();
        let out = parts.len().saturating_sub(1).checked_mul(r.len()).and_then(|n| n.checked_add(kept));
        bounded_growth(cx, "replace", s.len() + r.len(), out)?;
        Ok(Value::from(parts.join(r)))
    };
    "reverse" 1..=1 => |a, _| erl_string::reverse_latin1(s_arg(&a[0])?)
        .map(|b| Value::from_bytes(&b.into()))
        .ok_or_else(|| EvalError::new(
            "reverse() of a string with a character above U+00FF (EMQX writes each \
             character as one byte, and fails on these)",
        ));
    "rm_prefix" 2..=2 => |a, _| {
        let (s, p) = (s_bytes(&a[0])?, bin(&a[1])?);
        Ok(Value::from_bytes(&s.strip_prefix(p).unwrap_or(s).to_vec().into()))
    };
    "rtrim" 1..=2 => |a, _| {
        let s = s_arg(&a[0])?;
        Ok(Value::from(match a.get(1) {
            None => erl_string::trim_trailing(s, erl_string::WHITESPACE),
            Some(chars) => erl_string::trim_trailing(s, &code_points(text(chars)?)),
        }))
    };
    "split" 2..=3 => |a, _| {
        let (s, sep) = (s_arg(&a[0])?, text(&a[1])?);
        let opt = direction(
            a.get(2),
            &["trim", "notrim", "leading", "leading_notrim", "trailing", "trailing_notrim"],
        )?;
        let at = if opt.starts_with("leading") {
            erl_string::Where::Leading
        } else if opt.starts_with("trailing") {
            erl_string::Where::Trailing
        } else {
            erl_string::Where::All
        };
        let mut parts = erl_string::split(s, sep, at);
        if !opt.ends_with("notrim") {
            parts.retain(|p| !p.is_empty());
        }
        Ok(Value::from(parts.into_iter().map(Value::from).collect::<Vec<_>>()))
    };
    "sprintf" 1..=MANY => |a, cx| sprintf(cx, &a[0], &a[1..]);
    "sprintf_s" 2..=2 => |a, cx| sprintf(cx, &a[0], array(&a[1])?);
    "strlen" 1..=1 => |a, _| Ok(Value::Int(i64::try_from(erl_string::length(s_arg(&a[0])?)).unwrap_or(i64::MAX)));
    "substr" 2..=3 => |a, _| {
        let s = s_arg(&a[0])?;
        let count = |v: &Value, what: &str| {
            usize::try_from(int(v)?).map_err(|_| EvalError::new(format!("substr() {what} must be >= 0")))
        };
        let start = count(&a[1], "start")?;
        let len = a.get(2).map(|l| count(l, "length")).transpose()?;
        Ok(Value::from(erl_string::slice(s, start, len)))
    };
    "tokens" 2..=3 => |a, _| {
        let (s, seps) = (s_bytes(&a[0])?, bin(&a[1])?);
        let extra: &[&[u8]] = if direction(a.get(2), &["", "nocrlf"])? == "nocrlf" {
            &[b"\r", b"\n", b"\r\n"]
        } else {
            &[]
        };
        Ok(Value::from(
            erl_string::lexemes_latin1(s, seps, extra)
                .into_iter()
                .map(|t| Value::from_bytes(&t.to_vec().into()))
                .collect::<Vec<_>>(),
        ))
    };
    "trim" 1..=1 => |a, _| Ok(Value::from(erl_string::trim(s_arg(&a[0])?, erl_string::WHITESPACE)));
    "unescape" 1..=1 => |a, _| unescape(s_arg(&a[0])?).map(Value::from);
    "upper" 1..=1 => |a, _| Ok(Value::from(s_arg(&a[0])?.to_uppercase()));

    // -- map operations
    "map_new" 0..=0 => |_, _| Ok(Value::from(Map::new()));
    "map_get" 2..=3 => |a, _| {
        let path = dotted(text(&a[0])?);
        Ok(get_path(&decode_if_text(&a[1]), &path).unwrap_or_else(|| a.get(2).cloned().unwrap_or_default()))
    };
    "map_put" 3..=3 => |a, _| Ok(put_path(decode_if_text(&a[2]), &bounded_path(dotted(text(&a[0])?))?, a[1].clone()));
    "mget" 2..=3 => |a, _| {
        let path = key_list(&a[0])?;
        Ok(get_path(&decode_if_text(&a[1]), &path).unwrap_or_else(|| a.get(2).cloned().unwrap_or_default()))
    };
    "mput" 3..=3 => |a, _| Ok(put_path(decode_if_text(&a[2]), &bounded_path(key_list(&a[0])?)?, a[1].clone()));
    "map_keys" 1..=1 => |a, _| Ok(Value::from(map(&a[0])?.iter().map(|(k, _)| Value::Str(k.clone())).collect::<Vec<_>>()));
    "map_values" 1..=1 => |a, _| Ok(Value::from(map(&a[0])?.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>()));
    "map_size" 1..=1 => |a, _| Ok(Value::Int(i64::try_from(map(&a[0])?.len()).unwrap_or(i64::MAX)));
    "map_to_entries" 1..=1 => |a, _| Ok(Value::from(
        map(&a[0])?
            .iter()
            .map(|(k, v)| {
                let mut e = Map::with_capacity(2);
                e.insert("key", Value::Str(k.clone()));
                e.insert("value", v.clone());
                Value::from(e)
            })
            .collect::<Vec<_>>(),
    ));

    // -- array operations
    "contains" 2..=2 => |a, _| Ok(Value::Bool(array(&a[1])?.iter().any(|x| same(x, &a[0]))));
    "first" 1..=1 => |a, _| array(&a[0])?.first().cloned().ok_or_else(|| EvalError::new("first([]) is undefined"));
    "last" 1..=1 => |a, _| array(&a[0])?.last().cloned().ok_or_else(|| EvalError::new("last([]) is undefined"));
    "length" 1..=1 => |a, _| Ok(Value::Int(i64::try_from(array(&a[0])?.len()).unwrap_or(i64::MAX)));
    "nth" 2..=2 => |a, _| {
        let list = array(&a[1])?;
        let n = int(&a[0])?;
        if n < 1 {
            return Err(EvalError::new("nth() positions start at 1"));
        }
        resolve_index(n, list.len())
            .map(|i| list[i].clone())
            .ok_or_else(|| EvalError::new(format!("nth({n}) is past the end of a {}-element array", list.len())))
    };
    "sublist" 2..=3 => |a, _| {
        let (start, len, list) = match a {
            [len, list] => (1, int(len)?, array(list)?),
            [start, len, list] => (int(start)?, int(len)?, array(list)?),
            _ => unreachable!("arity checked at load"),
        };
        if start < 1 || len < 0 {
            return Err(EvalError::new("sublist() takes start >= 1 and length >= 0"));
        }
        let start = usize::try_from(start - 1).unwrap_or(usize::MAX);
        let len = usize::try_from(len).unwrap_or(usize::MAX);
        Ok(Value::from(list.iter().skip(start).take(len).cloned().collect::<Vec<_>>()))
    };

    // -- hashing
    "md5" 1..=1 => |a, _| Ok(Value::from(mqtt_core::hex_lower(&md5(bin(&a[0])?))));
    "sha" 1..=1 => |a, _| Ok(Value::from(digest(&aws_lc_rs::digest::SHA1_FOR_LEGACY_USE_ONLY, bin(&a[0])?)));
    "sha256" 1..=1 => |a, _| Ok(Value::from(digest(&aws_lc_rs::digest::SHA256, bin(&a[0])?)));
    "hash_to_range" 3..=3 => |a, _| {
        let h = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bin(&a[0])?);
        range_map(h.as_ref(), &a[1], &a[2])
    };
    "map_to_range" 3..=3 => |a, _| match &a[0] {
        Value::Int(n) => range_map(&n.to_be_bytes(), &a[1], &a[2]).and_then(|v| {
            // A negative integer maps by its value, not its two's-complement bytes.
            // In i128: the span of an i64 range overflows an i64. `range_map` has
            // already checked lo <= hi, so the result lies in [lo, hi].
            let (lo, hi) = (i128::from(int(&a[1])?), i128::from(int(&a[2])?));
            if *n < 0 {
                let r = lo + i128::from(*n).rem_euclid(hi - lo + 1);
                Ok(Value::Int(i64::try_from(r).map_err(|_| overflow())?))
            } else {
                Ok(v)
            }
        }),
        v => {
            let b = bin(v)?;
            if b.is_empty() {
                return Err(EvalError::new("map_to_range() needs a non-empty string"));
            }
            range_map(b, &a[1], &a[2])
        }
    };

    // -- bit operations
    "bitand" 2..=2 => |a, _| Ok(Value::Int(int(&a[0])? & int(&a[1])?));
    "bitor" 2..=2 => |a, _| Ok(Value::Int(int(&a[0])? | int(&a[1])?));
    "bitxor" 2..=2 => |a, _| Ok(Value::Int(int(&a[0])? ^ int(&a[1])?));
    "bitnot" 1..=1 => |a, _| Ok(Value::Int(!int(&a[0])?));
    "bitsl" 2..=2 => |a, _| {
        let (n, s) = (int(&a[0])?, u32::try_from(int(&a[1])?).map_err(|_| EvalError::new("shift must be >= 0"))?);
        let r = n.checked_shl(s).ok_or_else(overflow)?;
        if r >> s != n { return Err(overflow()); }
        Ok(Value::Int(r))
    };
    "bitsr" 2..=2 => |a, _| {
        let (n, s) = (int(&a[0])?, int(&a[1])?);
        let s = u32::try_from(s.clamp(0, 63)).unwrap_or(63);
        Ok(Value::Int(n >> s))
    };

    // -- encoding and decoding
    "base64_encode" 1..=3 => |a, _| {
        let engine = base64_engine(&a[1..])?;
        Ok(Value::from(engine.encode(bin(&a[0])?)))
    };
    "base64_decode" 1..=3 => |a, _| {
        let engine = base64_engine(&a[1..])?;
        let raw = engine.decode(bin(&a[0])?).map_err(|e| EvalError::new(format!("invalid base64: {e}")))?;
        Ok(Value::from_bytes(&raw.into()))
    };
    "json_decode" 1..=1 => |a, _| json_decode(bin(&a[0])?);
    "json_encode" 1..=1 => |a, _| Ok(Value::from(a[0].to_json()?));
    "bin2hexstr" 1..=2 => |a, _| {
        let hex = mqtt_core::hex_lower(bin(&a[0])?).to_uppercase();
        Ok(Value::from(match hex_prefix(a.get(1))? {
            Some(p) => format!("{p}{hex}"),
            None => hex,
        }))
    };
    "hexstr2bin" 1..=2 => |a, _| {
        let s = bin(&a[0])?;
        let digits = match hex_prefix(a.get(1))? {
            None => s,
            Some(p) => s
                .strip_prefix(p.as_bytes())
                .ok_or_else(|| EvalError::new(format!("the string does not start with '{p}'")))?,
        };
        Ok(Value::from_bytes(&hex_decode(digits)?.into()))
    };
    "sqlserver_bin2hexstr" 1..=1 => |a, _| Ok(Value::from(format!("0x{}", mqtt_core::hex_lower(bin(&a[0])?).to_uppercase())));

    // -- date and time
    "now_timestamp" 0..=1 => |a, _| Ok(Value::Int(scale_from_nanos(now_nanos(), unit(a.first())?)));
    "now_rfc3339" 0..=1 => |a, _| rfc3339(now_nanos(), unit(a.first())?);
    "unix_ts_to_rfc3339" 1..=2 => |a, _| {
        let u = unit(a.get(1))?;
        rfc3339(i128::from(int(&a[0])?) * nanos_per(u), u)
    };
    "rfc3339_to_unix_ts" 1..=2 => |a, _| {
        let t = chrono::DateTime::parse_from_rfc3339(text(&a[0])?)
            .map_err(|e| EvalError::new(format!("not an RFC 3339 time: {e}")))?;
        Ok(Value::Int(scale_from_nanos(datetime_nanos(&t), unit(a.get(1))?)))
    };
    "timezone_to_offset_seconds" 1..=1 => |a, _| Ok(Value::Int(i64::from(offset_seconds(&a[0])?)));
    // EMQX keeps the older name as an alias.
    "timezone_to_second" 1..=1 => |a, _| Ok(Value::Int(i64::from(offset_seconds(&a[0])?)));
    // Without the time, `format_date/3` formats now.
    "format_date" 3..=4 => |a, _| {
        let u = unit(Some(&a[0]))?;
        let offset = chrono::FixedOffset::east_opt(offset_seconds(&a[1])?)
            .ok_or_else(|| EvalError::new("time zone offset out of range"))?;
        let nanos = match a.get(3) {
            Some(t) => i128::from(int(t)?) * nanos_per(u),
            None => i128::from(scale_from_nanos(now_nanos(), u)) * nanos_per(u),
        };
        let t = utc_from_nanos(nanos)?.with_timezone(&offset);
        format_time(&t, text(&a[2])?).map(Value::from)
    };
    "date_to_unix_ts" 3..=4 => |a, _| {
        let u = unit(Some(&a[0]))?;
        let (offset, fmt, input) = match a {
            [_, f, s] => (None, text(f)?, text(s)?),
            [_, o, f, s] => (Some(offset_seconds(o)?), text(f)?, text(s)?),
            _ => unreachable!("arity checked at load"),
        };
        Ok(Value::Int(scale_from_nanos(parse_time(fmt, input, offset)?, u)))
    };

    // -- uuid
    "uuid_v4" 0..=0 => |_, _| uuid_v4(true).map(Value::from);
    "uuid_v4_no_hyphen" 0..=0 => |_, _| uuid_v4(false).map(Value::from);

    // -- bit sequences
    "bitsize" 1..=1 => |a, _| Ok(Value::Int(
        i64::try_from(bin(&a[0])?.len()).ok().and_then(|n| n.checked_mul(8)).ok_or_else(overflow)?,
    ));
    "bytesize" 1..=1 => |a, _| bytesize(&a[0]);
    "subbits" 2..=6 => |a, _| subbits(a);

    // -- compression
    "gzip" 1..=1 => |a, cx| compress::deflate(cx, "gzip", bin(&a[0])?, compress::Wrap::Gzip);
    "gunzip" 1..=1 => |a, cx| compress::inflate(cx, "gunzip", bin(&a[0])?, compress::Wrap::Gzip);
    "zip" 1..=1 => |a, cx| compress::deflate(cx, "zip", bin(&a[0])?, compress::Wrap::Raw);
    "unzip" 1..=1 => |a, cx| compress::inflate(cx, "unzip", bin(&a[0])?, compress::Wrap::Raw);
    "zip_compress" 1..=1 => |a, cx| compress::deflate(cx, "zip_compress", bin(&a[0])?, compress::Wrap::Zlib);
    "zip_uncompress" 1..=1 => |a, cx| compress::inflate(cx, "zip_uncompress", bin(&a[0])?, compress::Wrap::Zlib);
    "lz4_compress" 1..=1 => |a, cx| compress::lz4_compress(cx, bin(&a[0])?);
    "lz4_uncompress" 1..=1 => |a, cx| compress::lz4_uncompress(cx, bin(&a[0])?);

    // -- callable in EMQX though its reference does not list them: every export of
    //    emqx_rule_funcs is a SQL function there.
    // `div(a, b)` / `mod(a, b)`: the integer operators in call form (`mod` is `rem`).
    "div" 2..=2 => |a, _| {
        let (x, y) = (int(&a[0])?, int(&a[1])?);
        if y == 0 {
            return Err(EvalError::new("division by zero"));
        }
        x.checked_div(y).map(Value::Int).ok_or_else(overflow)
    };
    "mod" 2..=2 => |a, _| {
        let (x, y) = (int(&a[0])?, int(&a[1])?);
        if y == 0 {
            return Err(EvalError::new("division by zero"));
        }
        x.checked_rem(y).map(Value::Int).ok_or_else(overflow)
    };
    // Erlang `==`.
    "eq" 2..=2 => |a, _| Ok(Value::Bool(a[0].loose_eq(&a[1])));
    "null" 0..=0 => |_, _| Ok(Value::Undefined);
    "hash" 2..=2 => |a, _| hash(&a[0], &a[1]);
    "getenv" 1..=1 => |a, _| getenv(bin(&a[0])?);
    "map_to_redis_hset_args" 1..=1 => |a, _| Ok(redis_hset_args(&a[0]));
    "join_to_sql_values_string" 1..=1 => |a, _| sql_values(array(&a[0])?);
    // EMQX matches topic filters given as maps with the ATOM key `topic`, which no rule
    // value has (a decoded JSON object's keys are strings): every list gives `false`, and
    // anything else fails.
    "contains_topic" 2..=3 => |a, _| array(&a[0]).map(|_| Value::Bool(false));
    "contains_topic_match" 2..=3 => |a, _| array(&a[0]).map(|_| Value::Bool(false));

    // -- conditional
    "coalesce" 1..=MANY => |a, _| Ok(candidates(a).into_iter().find(|v| !v.is_undefined()).unwrap_or(Value::Null));
    "coalesce_ne" 1..=MANY => |a, _| Ok(candidates(a)
        .into_iter()
        .find(|v| !v.is_undefined() && v.as_bytes().is_none_or(|b| !b.is_empty()))
        .unwrap_or(Value::Null));

    // -- legacy accessors (EMQX keeps these for 4.x-era rules)
    "topic" 0..=1 => |a, cx| {
        let topic = cx.ctx.input.field("topic");
        match a.first() {
            None => Ok(topic),
            Some(n) => {
                let levels: Vec<&str> = topic.as_str().unwrap_or_default().split('/').collect();
                let n = usize::try_from(int(n)?).unwrap_or(0);
                levels.get(n.wrapping_sub(1)).map(|l| Value::from(*l))
                    .ok_or_else(|| EvalError::new("topic level out of range"))
            }
        }
    };
    "clientid" 0..=0 => |_, cx| Ok(cx.ctx.input.field("clientid"));
    "username" 0..=0 => |_, cx| Ok(cx.ctx.input.field("username"));
    "qos" 0..=0 => |_, cx| Ok(cx.ctx.input.field("qos"));
    "msgid" 0..=0 => |_, cx| Ok(cx.ctx.input.field("id"));
    "flags" 0..=0 => |_, cx| Ok(cx.ctx.input.field("flags"));
    "flag" 1..=1 => |a, cx| {
        let name = text(&a[0])?;
        Ok(get_path(&cx.ctx.input.field("flags"), &[Arc::from(name)]).unwrap_or_default())
    };
    "peerhost" 0..=0 => |_, cx| Ok(cx.ctx.input.field("peerhost"));
    "clientip" 0..=0 => |_, cx| Ok(cx.ctx.input.field("peerhost"));
    "payload" 0..=1 => |a, cx| {
        let p = cx.ctx.input.field("payload");
        match a.first() {
            None => Ok(p),
            Some(path) => Ok(get_path(&decode_if_text(&p), &dotted(text(path)?)).unwrap_or_default()),
        }
    };
}

/// Look up a function by name.
pub(crate) fn lookup(name: &str) -> Option<&'static Func> {
    FUNCS.iter().find(|f| f.name == name)
}

/// Every built-in function name, for documentation and its drift test.
#[must_use]
pub fn names() -> Vec<&'static str> {
    FUNCS.iter().map(|f| f.name).collect()
}

/// A pattern compiled, or why it does not compile.
pub(crate) type Compiled = Result<Arc<Regex>, String>;

/// A rule-supplied regular expression, compiled as Erlang's `re` compiles it (see
/// [`re`]). Its size is bounded by PCRE2 itself, as in OTP: a compiled pattern holds at
/// most 64K code units (`LINK_SIZE` 2), past which it is `regular expression is too
/// large`.
pub(crate) fn compile_regex(pattern: &[u8]) -> Compiled {
    Regex::new(pattern).map(Arc::new)
}

/// How many patterns taken from payloads one message remembers compiled. Four covers
/// rules that apply a couple of payload patterns per `FOREACH` element.
const REGEX_CACHE: usize = 4;

/// The function's pattern: compiled at load when it is a literal; otherwise compiled
/// here and remembered for the rest of the message, so a pattern taken from the payload
/// and applied per `FOREACH` element is compiled once, not once per element. A pattern
/// that does not compile fails the call, as `re:run` raises `badarg` in EMQX.
fn regex(cx: &FnCtx, pattern: &Value) -> Result<Arc<Regex>, EvalError> {
    if let Some(re) = cx.regex {
        return re.clone().map_err(EvalError::new);
    }
    let p = bin(pattern)?;
    let mut cache = cx.ctx.regex_cache.borrow_mut();
    if let Some(i) = cache.iter().position(|(cached, _)| cached.as_slice() == p) {
        let hit = cache.remove(i);
        let compiled = hit.1.clone();
        cache.insert(0, hit);
        return compiled.map_err(EvalError::new);
    }
    let compiled = compile_regex(p);
    cache.insert(0, (p.to_vec(), compiled.clone()));
    cache.truncate(REGEX_CACHE);
    compiled.map_err(EvalError::new)
}

/// A group of a match as a value: its bytes, or `''` when it did not take part.
fn group_value(s: &[u8], group: Option<(usize, usize)>) -> Value {
    group.map_or_else(
        || Value::from(""),
        |(start, end)| Value::from_bytes(&bytes::Bytes::copy_from_slice(&s[start..end])),
    )
}

/// `re:replace` with `global`: every match `re:run` reports, replaced in order
/// (`do_mlist/5`), refused once the output would take the message past its
/// [`MAX_BUILT_BYTES`] budget. Checked before each expansion, against an upper bound on
/// it: the replacement's literal text plus, for every group reference in it, the whole
/// match. A match reported before the end of the previous one fails the call, as it
/// fails `do_mlist/5`.
fn bounded_replace_all(
    cx: &FnCtx,
    re: &Regex,
    s: &[u8],
    rep: &[RepPart],
) -> Result<Value, EvalError> {
    let literal: usize = rep
        .iter()
        .map(|p| match p {
            RepPart::Lit(l) => l.len(),
            RepPart::Group(_) => 0,
        })
        .sum();
    let refs = rep
        .len()
        .saturating_sub(rep.iter().filter(|p| matches!(p, RepPart::Lit(_))).count());
    let input = s.len().saturating_add(literal);
    let limit = input.saturating_add(MAX_BUILT_BYTES.saturating_sub(cx.ctx.built.get()));
    let mut out: Vec<u8> = Vec::new();
    let mut last = 0;
    re.each(s, |m: &Groups| -> Result<(), EvalError> {
        let Some((start, end)) = m[0] else {
            return Ok(());
        };
        if start < last {
            return Err(EvalError::new(
                "a match starts before the previous one ends",
            ));
        }
        let worst = refs
            .checked_mul(end - start)
            .and_then(|n| n.checked_add(literal + (start - last) + out.len()));
        if worst.is_none_or(|n| n > limit) {
            bounded_growth(cx, "regex_replace", input, None)?;
        }
        out.extend_from_slice(&s[last..start]);
        for part in rep {
            match part {
                RepPart::Lit(l) => out.extend_from_slice(l),
                RepPart::Group(n) => {
                    if let Some(Some((gs, ge))) = m.get(*n) {
                        out.extend_from_slice(&s[*gs..*ge]);
                    }
                }
            }
        }
        last = end;
        Ok(())
    })?;
    out.extend_from_slice(&s[last..]);
    bounded_growth(cx, "regex_replace", input, Some(out.len()))?;
    Ok(Value::from_bytes(&out.into()))
}

/// One piece of an `re:replace` replacement.
#[derive(Debug, PartialEq, Eq)]
enum RepPart {
    /// Bytes copied as they are.
    Lit(Vec<u8>),
    /// A group's text; nothing when the group did not take part or does not exist.
    Group(usize),
}

/// Erlang `re:replace`'s replacement syntax, exactly as OTP's `re:precomp_repl/1` reads
/// it: `&` and `\g{0}` are the whole match; `\N`, `\gN` and `\g{N}` are group N (`\N`
/// needs N to start 1-9, and takes every digit that follows); a backslash before anything
/// else is that character itself (`\0` is `0`, `\&` is `&`, `\\` is `\`); a backslash at
/// the end is itself; and `\g` before a non-digit, or `\g{` without digits and a closing
/// brace, is an error. `$` means nothing.
fn erlang_replacement(rep: &[u8]) -> Result<Vec<RepPart>, EvalError> {
    let bad = || EvalError::new("bad \\g reference in the replacement");
    let mut parts: Vec<RepPart> = Vec::new();
    let lit = |parts: &mut Vec<RepPart>, b: u8| match parts.last_mut() {
        Some(RepPart::Lit(l)) => l.push(b),
        _ => parts.push(RepPart::Lit(vec![b])),
    };
    // Digits from `at`, as a group number (one too large to exist reads as no group).
    let digits = |at: usize| -> (usize, usize) {
        let n = rep[at..].iter().take_while(|b| b.is_ascii_digit()).count();
        let num = std::str::from_utf8(&rep[at..at + n])
            .ok()
            .and_then(|d| d.parse().ok())
            .unwrap_or(usize::MAX);
        (num, n)
    };
    let mut at = 0;
    while at < rep.len() {
        match (rep[at], rep.get(at + 1), rep.get(at + 2)) {
            (b'\\', Some(b'g'), Some(b'{')) if rep.len() > at + 3 => {
                let (num, n) = digits(at + 3);
                if n == 0 || rep.get(at + 3 + n) != Some(&b'}') {
                    return Err(bad());
                }
                parts.push(RepPart::Group(num));
                at += 4 + n;
            }
            (b'\\', Some(b'g'), Some(_)) => {
                let (num, n) = digits(at + 2);
                if n == 0 {
                    return Err(bad());
                }
                parts.push(RepPart::Group(num));
                at += 2 + n;
            }
            (b'\\', Some(&d), _) if (b'1'..=b'9').contains(&d) => {
                let (num, n) = digits(at + 1);
                parts.push(RepPart::Group(num));
                at += 1 + n;
            }
            (b'\\', Some(&x), _) => {
                lit(&mut parts, x);
                at += 2;
            }
            (b'&', _, _) => {
                parts.push(RepPart::Group(0));
                at += 1;
            }
            (b, _, _) => {
                lit(&mut parts, b);
                at += 1;
            }
        }
    }
    Ok(parts)
}

fn overflow() -> EvalError {
    EvalError::new("integer overflow")
}

fn type_err(wanted: &str, got: &Value) -> EvalError {
    EvalError::new(format!("expected {wanted}, got a {}", got.type_name()))
}

fn num(v: &Value) -> Result<f64, EvalError> {
    v.as_f64().ok_or_else(|| type_err("a number", v))
}

fn int(v: &Value) -> Result<i64, EvalError> {
    match v {
        Value::Int(n) => Ok(*n),
        v => Err(type_err("an integer", v)),
    }
}

fn text(v: &Value) -> Result<&str, EvalError> {
    v.as_str().ok_or_else(|| type_err("a string", v))
}

fn bin(v: &Value) -> Result<&[u8], EvalError> {
    v.as_bytes().ok_or_else(|| type_err("a string", v))
}

fn array(v: &Value) -> Result<&[Value], EvalError> {
    match v {
        Value::Array(a) => Ok(a),
        v => Err(type_err("an array", v)),
    }
}

fn map(v: &Value) -> Result<&Map, EvalError> {
    match v {
        Value::Map(m) => Ok(m),
        v => Err(type_err("a map", v)),
    }
}

fn math(a: &[Value], f: fn(f64) -> f64) -> Result<Value, EvalError> {
    Value::float(f(num(&a[0])?))
}

fn to_int_value(f: f64) -> Result<Value, EvalError> {
    if f.is_finite() && (-9.223_372_036_854_775e18..=9.223_372_036_854_775e18).contains(&f) {
        // Range-checked just above.
        #[allow(clippy::cast_possible_truncation)]
        Ok(Value::Int(f as i64))
    } else {
        Err(overflow())
    }
}

fn to_float(v: &Value) -> Result<f64, EvalError> {
    match v {
        v if v.is_number() => num(v),
        v => v
            .as_str()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .filter(|f| f.is_finite())
            .ok_or_else(|| {
                EvalError::new(format!(
                    "cannot convert {} to a float",
                    v.to_text().unwrap_or_default()
                ))
            }),
    }
}

fn to_int(v: &Value) -> Result<i64, EvalError> {
    match v {
        Value::Int(n) => Ok(*n),
        Value::Float(f) => match to_int_value(f.floor())? {
            Value::Int(n) => Ok(n),
            _ => Err(overflow()),
        },
        Value::Bool(b) => Ok(i64::from(*b)),
        v => {
            let s = v
                .as_str()
                .ok_or_else(|| type_err("a number, boolean or numeric string", v))?
                .trim();
            if let Ok(n) = s.parse::<i64>() {
                return Ok(n);
            }
            match s.parse::<f64>().ok().filter(|f| f.is_finite()) {
                Some(f) => to_int(&Value::Float(f)),
                None => Err(EvalError::new(format!(
                    "cannot convert '{s}' to an integer"
                ))),
            }
        }
    }
}

/// Trim trailing zeros the way Erlang's `compact` float option does.
fn compact_decimals(s: &str) -> String {
    if !s.contains('.') {
        return s.to_string();
    }
    let t = s.trim_end_matches('0');
    if t.ends_with('.') {
        format!("{t}0")
    } else {
        t.to_string()
    }
}

/// An optional direction/option argument, defaulting to `allowed[0]`.
fn direction<'a>(v: Option<&'a Value>, allowed: &[&'static str]) -> Result<&'a str, EvalError> {
    match v {
        None => Ok(allowed[0]),
        Some(v) => {
            let s = text(v)?;
            if allowed.contains(&s) {
                Ok(s)
            } else {
                Err(EvalError::new(format!(
                    "'{s}' is not one of {}",
                    allowed.join(", ")
                )))
            }
        }
    }
}

/// A string that holds JSON is read as the JSON it holds (EMQX accepts JSON text
/// wherever it accepts a map).
fn decode_if_text(v: &Value) -> Value {
    match v {
        Value::Str(_) | Value::Bin(_) => {
            json_decode(v.as_bytes().unwrap_or_default()).unwrap_or_else(|_| v.clone())
        }
        other => other.clone(),
    }
}

/// `map_get` keys are dotted paths.
fn dotted(key: &str) -> Vec<Arc<str>> {
    key.split('.').map(Arc::from).collect()
}

/// `mget` / `mput` keys: one key, or an array of keys for a nested path.
fn key_list(v: &Value) -> Result<Vec<Arc<str>>, EvalError> {
    match v {
        Value::Array(a) => a.iter().map(|k| Ok(Arc::from(k.to_text()?))).collect(),
        k => Ok(vec![Arc::from(k.to_text()?)]),
    }
}

fn get_path(v: &Value, path: &[Arc<str>]) -> Option<Value> {
    let mut cur = v.clone();
    for k in path {
        cur = match decode_if_text(&cur) {
            Value::Map(m) => m.get(k)?.clone(),
            _ => return None,
        };
    }
    Some(cur)
}

/// A `map_put` / `mput` path, refused past [`MAX_PATH_SEGMENTS`].
fn bounded_path(path: Vec<Arc<str>>) -> Result<Vec<Arc<str>>, EvalError> {
    if path.len() > MAX_PATH_SEGMENTS {
        return Err(EvalError::new(format!(
            "a key path of {} segments is longer than {MAX_PATH_SEGMENTS}",
            path.len()
        )));
    }
    Ok(path)
}

fn put_path(target: Value, path: &[Arc<str>], v: Value) -> Value {
    let Some((k, rest)) = path.split_first() else {
        return v;
    };
    let mut m = match target {
        Value::Map(m) => Arc::unwrap_or_clone(m),
        _ => Map::new(),
    };
    let child = m.remove(k).unwrap_or_default();
    m.insert(k.clone(), put_path(decode_if_text(&child), rest, v));
    Value::from(m)
}

/// Erlang `lists:member` equality: exact, so `2` is not a member of `[2.0]`.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(_), Value::Float(_)) | (Value::Float(_), Value::Int(_)) => false,
        _ => a.loose_eq(b),
    }
}

fn candidates(a: &[Value]) -> Vec<Value> {
    match a {
        [Value::Array(list)] => list.as_ref().clone(),
        _ => a.to_vec(),
    }
}

fn digest(alg: &'static aws_lc_rs::digest::Algorithm, data: &[u8]) -> String {
    mqtt_core::hex_lower(aws_lc_rs::digest::digest(alg, data).as_ref())
}

/// Map `bytes`, read as an unsigned big-endian integer, into `[lo, hi]`.
fn range_map(bytes: &[u8], lo: &Value, hi: &Value) -> Result<Value, EvalError> {
    let (lo, hi) = (int(lo)?, int(hi)?);
    if lo > hi {
        return Err(EvalError::new("range minimum must not exceed its maximum"));
    }
    let span = u128::try_from(i128::from(hi) - i128::from(lo) + 1).map_err(|_| overflow())?;
    // (a·256 + b) mod n, folded byte by byte, is the big integer's remainder.
    let rem = bytes
        .iter()
        .fold(0u128, |acc, b| (acc * 256 + u128::from(*b)) % span);
    // lo + rem <= hi, so it fits; the sum is taken in i128 because rem alone may not.
    let v = i128::from(lo) + i128::try_from(rem).map_err(|_| overflow())?;
    Ok(Value::Int(i64::try_from(v).map_err(|_| overflow())?))
}

/// `emqx_utils:hexstr_to_bin/1`: two digits per byte, an odd count read as if it had a
/// leading `0` (`abc` is `0A BC`).
fn hex_decode(s: &[u8]) -> Result<Vec<u8>, EvalError> {
    let digit = |c: u8| {
        char::from(c)
            .to_digit(16)
            .and_then(|d| u8::try_from(d).ok())
            .ok_or_else(|| EvalError::new("not a hex string"))
    };
    let (head, rest) = if s.len().is_multiple_of(2) {
        (None, s)
    } else {
        (Some(digit(s[0])?), &s[1..])
    };
    head.into_iter()
        .map(Ok)
        .chain(
            rest.chunks_exact(2)
                .map(|p| Ok(digit(p[0])? * 16 + digit(p[1])?)),
        )
        .collect()
}

/// The prefix argument of `bin2hexstr/2` and `hexstr2bin/2`: a binary, or `undefined`
/// for none.
fn hex_prefix(v: Option<&Value>) -> Result<Option<&str>, EvalError> {
    match v {
        None | Some(Value::Undefined) => Ok(None),
        Some(v) => text(v).map(Some),
    }
}

fn base64_engine(opts: &[Value]) -> Result<base64::engine::GeneralPurpose, EvalError> {
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    let (mut url, mut no_pad) = (false, false);
    for o in opts {
        match text(o)? {
            "urlsafe" => url = true,
            "no_padding" => no_pad = true,
            other => return Err(EvalError::new(format!("unknown base64 option '{other}'"))),
        }
    }
    Ok(match (url, no_pad) {
        (false, false) => STANDARD,
        (false, true) => STANDARD_NO_PAD,
        (true, false) => URL_SAFE,
        (true, true) => URL_SAFE_NO_PAD,
    })
}

fn random_bytes(buf: &mut [u8]) -> Result<(), EvalError> {
    aws_lc_rs::rand::fill(buf).map_err(|_| EvalError::new("the system random source failed"))
}

fn uuid_v4(hyphens: bool) -> Result<String, EvalError> {
    let mut b = [0u8; 16];
    random_bytes(&mut b)?;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h = mqtt_core::hex_lower(&b);
    Ok(if hyphens {
        format!(
            "{}-{}-{}-{}-{}",
            &h[0..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..32]
        )
    } else {
        h
    })
}

/// `sprintf(Format, Args...)` and `sprintf_s(Format, [Args])`: Erlang's `io_lib:format`
/// ([`io_format`]), its output charged to the message's growth budget.
fn sprintf(cx: &FnCtx, fmt: &Value, args: &[Value]) -> Result<Value, EvalError> {
    let fmt = bin(fmt)?;
    let input = args
        .iter()
        .filter_map(Value::as_bytes)
        .map(<[u8]>::len)
        .fold(fmt.len(), usize::saturating_add);
    let limit = input.saturating_add(MAX_BUILT_BYTES.saturating_sub(cx.ctx.built.get()));
    let out = io_format::format(fmt, args, limit)?;
    bounded_growth(cx, "sprintf", input, Some(out.len()))?;
    Ok(Value::from_bytes(&out.into()))
}

/// The first argument of EMQX's string functions (`emqx_variform_bif`): a string, or an
/// atom read as its name, so `lower(true)` is `true`; `null` and a missing value fail.
fn s_arg(v: &Value) -> Result<&str, EvalError> {
    match v {
        Value::Bool(true) => Ok("true"),
        Value::Bool(false) => Ok("false"),
        v => text(v),
    }
}

/// [`s_arg`] for the functions that work on bytes.
fn s_bytes(v: &Value) -> Result<&[u8], EvalError> {
    match v {
        Value::Bool(true) => Ok(b"true"),
        Value::Bool(false) => Ok(b"false"),
        v => bin(v),
    }
}

/// Each code point of `s` as a separator (`unicode:characters_to_list/2`).
fn code_points(s: &str) -> Vec<&str> {
    s.char_indices()
        .map(|(i, c)| &s[i..i + c.len_utf8()])
        .collect()
}

/// `erlang:iolist_size/1` and the data `crypto:hash/2` takes: a string, or an array of
/// strings, bytes (integers 0..=255) and such arrays.
fn iolist(v: &Value, out: &mut Vec<u8>) -> Result<(), EvalError> {
    match v {
        Value::Str(_) | Value::Bin(_) => out.extend_from_slice(v.as_bytes().unwrap_or_default()),
        Value::Int(b) => out.push(u8::try_from(*b).map_err(|_| type_err("a byte (0..255)", v))?),
        Value::Array(a) => {
            for x in a.iter() {
                iolist(x, out)?;
            }
        }
        other => return Err(type_err("a string or an array of strings and bytes", other)),
    }
    Ok(())
}

/// `hash(Algorithm, Data)`: any digest Erlang's `crypto:hash/2` offers, as lower-case hex.
/// `sha1` is an alias of `sha`; `shake128`/`shake256` give their default 128/256 bits.
fn hash(alg: &Value, data: &Value) -> Result<Value, EvalError> {
    use sha2::Digest as _;
    use shake::ExtendableOutput as _;
    let data = match data {
        Value::Null | Value::Undefined | Value::Int(_) => return Err(type_err("a string", data)),
        Value::Bool(b) => b.to_string().into_bytes(),
        v => {
            let mut out = Vec::new();
            iolist(v, &mut out)?;
            out
        }
    };
    let aws = |a: &'static aws_lc_rs::digest::Algorithm| {
        aws_lc_rs::digest::digest(a, &data).as_ref().to_vec()
    };
    let alg_name = text(alg)?;
    let digest: Vec<u8> = match alg_name {
        "md4" => md4::Md4::digest(&data).to_vec(),
        "md5" => md5(&data).to_vec(),
        "sha" | "sha1" => aws(&aws_lc_rs::digest::SHA1_FOR_LEGACY_USE_ONLY),
        "sha224" => aws(&aws_lc_rs::digest::SHA224),
        "sha256" => aws(&aws_lc_rs::digest::SHA256),
        "sha384" => aws(&aws_lc_rs::digest::SHA384),
        "sha512" => aws(&aws_lc_rs::digest::SHA512),
        "sha512_224" => sha2::Sha512_224::digest(&data).to_vec(),
        "sha512_256" => aws(&aws_lc_rs::digest::SHA512_256),
        "sha3_224" => sha3::Sha3_224::digest(&data).to_vec(),
        "sha3_256" => sha3::Sha3_256::digest(&data).to_vec(),
        "sha3_384" => sha3::Sha3_384::digest(&data).to_vec(),
        "sha3_512" => sha3::Sha3_512::digest(&data).to_vec(),
        "shake128" => {
            let mut out = vec![0u8; 16];
            shake::Shake128::digest_xof(&data, &mut out);
            out
        }
        "shake256" => {
            let mut out = vec![0u8; 32];
            shake::Shake256::digest_xof(&data, &mut out);
            out
        }
        "blake2b" => blake2::Blake2b512::digest(&data).to_vec(),
        "blake2s" => blake2::Blake2s256::digest(&data).to_vec(),
        "ripemd160" => ripemd::Ripemd160::digest(&data).to_vec(),
        "sm3" => sm3::Sm3::digest(&data).to_vec(),
        other => return Err(EvalError::new(format!("unknown hash algorithm '{other}'"))),
    };
    Ok(Value::from(mqtt_core::hex_lower(&digest)))
}

/// How many `getenv` results are remembered (EMQX keeps every one for the life of the
/// node; a name taken from the payload must not grow this without bound).
const GETENV_CACHE: usize = 1024;

/// `getenv(Name)`: the environment variable `EMQXVAR_<Name>`, `''` when unset. Only the
/// `EMQXVAR_` namespace is readable, so an operator exposes a value to rules by naming it
/// so, and nothing else in the broker's environment is reachable. As in EMQX, the name's
/// bytes are read as Latin-1 characters, the value's characters must each fit in a byte
/// (a Latin-1 byte for U+0080..U+00FF), and a value once read does not change.
fn getenv(name: &[u8]) -> Result<Value, EvalError> {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    static CACHE: OnceLock<Mutex<HashMap<Vec<u8>, Vec<u8>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(v) = cache.lock().ok().and_then(|c| c.get(name).cloned()) {
        return Ok(Value::from_bytes(&v.into()));
    }
    let key: String = "EMQXVAR_"
        .chars()
        .chain(name.iter().map(|b| char::from(*b)))
        .collect();
    if name.contains(&b'=') || name.contains(&0) {
        return Err(EvalError::new("a name cannot contain '=' or NUL"));
    }
    let value: Vec<u8> = match std::env::var_os(&key) {
        None => Vec::new(),
        Some(v) => match v.into_string() {
            Ok(s) => s
                .chars()
                .map(|c| u8::try_from(u32::from(c)))
                .collect::<Result<_, _>>()
                .map_err(|_| {
                    EvalError::new(format!(
                        "{key} holds a character above U+00FF, which EMQX cannot return"
                    ))
                })?,
            Err(raw) => raw.into_encoded_bytes(),
        },
    };
    if let Ok(mut c) = cache.lock() {
        if c.len() < GETENV_CACHE {
            c.insert(name.to_vec(), value.clone());
        }
    }
    Ok(Value::from_bytes(&value.into()))
}

/// `map_to_redis_hset_args(Map)`: `[map_to_redis_hset_args, K1, V1, ...]` — the marker
/// atom (a string here) first, then each pair whose value is a string, integer, float
/// (`float2str(V, 6)`) or boolean; others are dropped. A string is read as JSON; one that
/// is not a JSON object, or any other value, gives the marker alone. The pairs come in
/// the order EMQX's `maps:fold` leaves them: keys descending.
fn redis_hset_args(v: &Value) -> Value {
    let decoded;
    let map = match v {
        Value::Map(m) => Some(m),
        Value::Str(_) | Value::Bin(_) => {
            decoded = json_decode(v.as_bytes().unwrap_or_default()).ok();
            match &decoded {
                Some(Value::Map(m)) => Some(m),
                _ => None,
            }
        }
        _ => None,
    };
    let mut out = vec![Value::from("map_to_redis_hset_args")];
    if let Some(m) = map {
        let mut entries: Vec<_> = m.iter().collect();
        entries.sort_by(|a, b| b.0.as_bytes().cmp(a.0.as_bytes()));
        for (k, x) in entries {
            let field = match x {
                Value::Str(_) | Value::Bin(_) => x.clone(),
                Value::Int(n) => Value::from(n.to_string()),
                Value::Float(f) => Value::from(compact_decimals(&format!("{f:.6}"))),
                Value::Bool(b) => Value::from(b.to_string()),
                _ => continue,
            };
            out.push(Value::Str(k.clone()));
            out.push(field);
        }
    }
    Value::from(out)
}

/// `join_to_sql_values_string(List)`: each item as an SQL literal, joined by `, `. A
/// string, atom (`true`, `null`), map or array is quoted (maps and arrays as JSON) with
/// `\` and `'` backslash-escaped; a number is written bare; a missing value is `NULL`.
fn sql_values(list: &[Value]) -> Result<Value, EvalError> {
    let mut out: Vec<u8> = Vec::new();
    for (n, item) in list.iter().enumerate() {
        if n > 0 {
            out.extend_from_slice(b", ");
        }
        let quoted: Vec<u8> = match item {
            Value::Undefined => {
                out.extend_from_slice(b"NULL");
                continue;
            }
            Value::Int(_) | Value::Float(_) => {
                out.extend_from_slice(item.to_text()?.as_bytes());
                continue;
            }
            Value::Str(_) | Value::Bin(_) => item.as_bytes().unwrap_or_default().to_vec(),
            Value::Array(_) | Value::Map(_) => item.to_json()?.into_bytes(),
            other => other.to_text()?.into_bytes(),
        };
        out.push(b'\'');
        for b in quoted {
            if matches!(b, b'\\' | b'\'') {
                out.push(b'\\');
            }
            out.push(b);
        }
        out.push(b'\'');
    }
    Ok(Value::from_bytes(&out.into()))
}

/// `bytesize(Data)`: `erlang:iolist_size/1`.
fn bytesize(v: &Value) -> Result<Value, EvalError> {
    fn size(v: &Value) -> Result<usize, EvalError> {
        match v {
            Value::Str(_) | Value::Bin(_) => Ok(v.as_bytes().unwrap_or_default().len()),
            Value::Int(0..=255) => Ok(1),
            Value::Array(a) => a
                .iter()
                .map(size)
                .try_fold(0usize, |t, n| Ok(t.saturating_add(n?))),
            other => Err(type_err("a string or an array of strings and bytes", other)),
        }
    }
    if let Value::Int(_) = v {
        return Err(type_err("a string or an array of strings and bytes", v));
    }
    Ok(Value::Int(i64::try_from(size(v)?).unwrap_or(i64::MAX)))
}

/// `subbits(Bin, [Start,] Len[, Type[, Signedness[, Endianness]]])`: `Len` bits from bit
/// `Start` (1-based) as an integer, float or bit string, as EMQX's `get_subbits/6`. A
/// `Len` that is negative or runs past the end takes the bits to the end instead; a
/// `Start` outside the binary gives `undefined`. A float is 16, 32 or 64 bits and finite,
/// or the call fails.
///
/// Two results Erlang has cannot be represented and fail instead: an integer outside
/// 64-bit range, and a bit string whose length is not a whole number of bytes.
fn subbits(a: &[Value]) -> Result<Value, EvalError> {
    let data = bin(&a[0])?;
    let (start, len) = match a {
        [_, len] => (1, int(len)?),
        [_, start, len, ..] => (int(start)?, int(len)?),
        _ => unreachable!("arity checked at load"),
    };
    let total = i64::try_from(data.len())
        .unwrap_or(i64::MAX / 8)
        .saturating_mul(8);
    let begin = start.saturating_sub(1);
    if begin < 0 || begin >= total {
        return Ok(Value::Undefined);
    }
    let rest = total - begin;
    let ty = a.get(3).map(text).transpose()?.unwrap_or("integer");
    let signed = match a.get(4).map(text).transpose()?.unwrap_or("unsigned") {
        "unsigned" => false,
        "signed" => true,
        other => {
            return Err(EvalError::new(format!(
                "'{other}' is not signed or unsigned"
            )))
        }
    };
    let little = match a.get(5).map(text).transpose()?.unwrap_or("big") {
        "big" => false,
        "little" => true,
        other => return Err(EvalError::new(format!("'{other}' is not big or little"))),
    };
    let bit = |i: i64| -> u8 {
        let i = usize::try_from(begin + i).unwrap_or(0);
        (data[i / 8] >> (7 - i % 8)) & 1
    };
    // The bits in the order the value reads them, most significant first: a little-endian
    // field's bytes reversed, its final partial byte (if any) first.
    let ordered = |n: i64| -> Vec<u8> {
        let bits: Vec<u8> = (0..n).map(bit).collect();
        if !little {
            return bits;
        }
        let chunks: Vec<&[u8]> = bits.chunks(8).collect();
        chunks.into_iter().rev().flatten().copied().collect()
    };
    match ty {
        "integer" => {
            let n = if (0..=rest).contains(&len) { len } else { rest };
            int_from_bits(&ordered(n), signed)
        }
        "float" => {
            let try_float = |n: i64| -> Option<f64> {
                if !matches!(n, 16 | 32 | 64) || n > rest {
                    return None;
                }
                let raw = ordered(n)
                    .iter()
                    .fold(0u64, |acc, b| (acc << 1) | u64::from(*b));
                let f = match n {
                    16 => f16_to_f64(u16::try_from(raw).ok()?),
                    32 => f64::from(f32::from_bits(u32::try_from(raw).ok()?)),
                    _ => f64::from_bits(raw),
                };
                f.is_finite().then_some(f)
            };
            try_float(len)
                .or_else(|| try_float(rest))
                .map(Value::Float)
                .ok_or_else(|| EvalError::new("the bits are not a finite 16, 32 or 64-bit float"))
        }
        "bits" => {
            let n = if (0..=rest).contains(&len) { len } else { rest };
            if n % 8 != 0 {
                return Err(EvalError::new(format!(
                    "a {n}-bit string is not a whole number of bytes, which mqttd cannot represent"
                )));
            }
            let bytes: Vec<u8> = (0..n / 8)
                .map(|k| (0..8).fold(0u8, |acc, i| (acc << 1) | bit(k * 8 + i)))
                .collect();
            Ok(Value::from_bytes(&bytes.into()))
        }
        other => Err(EvalError::new(format!(
            "'{other}' is not integer, float or bits"
        ))),
    }
}

/// An integer from its bits, most significant first.
fn int_from_bits(bits: &[u8], signed: bool) -> Result<Value, EvalError> {
    let negative = signed && bits.first() == Some(&1);
    // A negative two's-complement value is -(inverted bits) - 1.
    let magnitude = bits.iter().try_fold(0u64, |acc, b| {
        let b = if negative { 1 - b } else { *b };
        acc.checked_mul(2).and_then(|a| a.checked_add(u64::from(b)))
    });
    let range =
        || EvalError::new("the integer is outside 64-bit range, which mqttd cannot represent");
    let m = i64::try_from(magnitude.ok_or_else(range)?).map_err(|_| range())?;
    Ok(Value::Int(if negative { -m - 1 } else { m }))
}

/// An IEEE 754 half-precision float.
fn f16_to_f64(h: u16) -> f64 {
    // Exact: a half's every value is a double, and these are products of powers of two.
    let pow2 = |e: i32| f64::from_bits(u64::try_from(1023 + e).unwrap_or(0) << 52);
    let sign = if h & 0x8000 == 0 { 1.0 } else { -1.0 };
    let exp = i32::from((h >> 10) & 0x1f);
    let frac = f64::from(h & 0x3ff);
    match exp {
        0 => sign * frac * pow2(-24),
        31 => {
            if frac == 0.0 {
                sign * f64::INFINITY
            } else {
                f64::NAN
            }
        }
        e => sign * (1.0 + frac / 1024.0) * pow2(e - 15),
    }
}

/// C escapes and `\xH…` hex escapes, as EMQX's `unescape/1`.
fn unescape(s: &str) -> Result<String, EvalError> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        // A backslash ending the string is kept, as EMQX's `unescape_string/2` keeps it.
        let Some(e) = chars.next() else {
            out.push('\\');
            break;
        };
        out.push(match e {
            'n' => '\n',
            't' => '\t',
            'r' => '\r',
            'b' => '\u{08}',
            'f' => '\u{0c}',
            'v' => '\u{0b}',
            'a' => '\u{07}',
            '\'' | '"' | '\\' | '?' => e,
            'x' => {
                let mut hex = String::new();
                while let Some(h) = chars.peek().filter(|h| h.is_ascii_hexdigit()) {
                    hex.push(*h);
                    chars.next();
                }
                u32::from_str_radix(&hex, 16)
                    .ok()
                    .and_then(char::from_u32)
                    .ok_or_else(|| EvalError::new("unescape: invalid \\x escape"))?
            }
            other => {
                return Err(EvalError::new(format!(
                    "unescape: unknown escape \\{other}"
                )))
            }
        });
    }
    Ok(out)
}

/// MD5 (RFC 1321) for EMQX's `md5/1` — a data checksum here, not a security
/// primitive, and the workspace crypto provider does not expose it. Variable names
/// follow the RFC.
#[allow(clippy::many_single_char_names)]
fn md5(input: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    // K[i] = floor(|sin(i + 1)| · 2^32)
    let k: Vec<u32> = (0..64u32)
        .map(|i| {
            // Exactly representable and < 2^32 by construction.
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let v = (f64::from(i + 1).sin().abs() * 4_294_967_296.0) as u32;
            v
        })
        .collect();
    let mut msg = input.to_vec();
    let bit_len = (input.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());
    let (mut a0, mut b0, mut c0, mut d0) = (
        0x6745_2301u32,
        0xefcd_ab89u32,
        0x98ba_dcfeu32,
        0x1032_5476u32,
    );
    for chunk in msg.chunks_exact(64) {
        let m: Vec<u32> = chunk
            .chunks_exact(4)
            .map(|w| u32::from_le_bytes([w[0], w[1], w[2], w[3]]))
            .collect();
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = match i / 16 {
                0 => ((b & c) | (!b & d), i),
                1 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                2 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let f = f.wrapping_add(a).wrapping_add(k[i]).wrapping_add(m[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(f.rotate_left(S[i]));
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }
    let mut out = [0u8; 16];
    for (i, w) in [a0, b0, c0, d0].iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&w.to_le_bytes());
    }
    out
}

// -- time

#[derive(Clone, Copy, PartialEq, Eq)]
enum Unit {
    Second,
    Milli,
    Micro,
    Nano,
}

fn unit(v: Option<&Value>) -> Result<Unit, EvalError> {
    Ok(match v.map(text).transpose()? {
        None | Some("second") => Unit::Second,
        Some("millisecond") => Unit::Milli,
        Some("microsecond") => Unit::Micro,
        Some("nanosecond") => Unit::Nano,
        Some(other) => {
            return Err(EvalError::new(format!(
                "unknown time unit '{other}' (second, millisecond, microsecond, nanosecond)"
            )))
        }
    })
}

fn nanos_per(u: Unit) -> i128 {
    match u {
        Unit::Second => 1_000_000_000,
        Unit::Milli => 1_000_000,
        Unit::Micro => 1_000,
        Unit::Nano => 1,
    }
}

fn now_nanos() -> i128 {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i128::try_from(d.as_nanos()).unwrap_or(i128::MAX)
}

fn scale_from_nanos(nanos: i128, u: Unit) -> i64 {
    i64::try_from(nanos.div_euclid(nanos_per(u))).unwrap_or(i64::MAX)
}

fn datetime_nanos<Tz: chrono::TimeZone>(t: &chrono::DateTime<Tz>) -> i128 {
    i128::from(t.timestamp()) * 1_000_000_000 + i128::from(t.timestamp_subsec_nanos())
}

/// A UTC time from nanoseconds since the epoch, refused unless chrono can also render
/// it in any offset: rendering adds the offset to the UTC time, and chrono PANICS when
/// that sum leaves its range — so the range's last two days at either end are refused
/// too. The timestamp is often a payload field.
fn utc_from_nanos(nanos: i128) -> Result<chrono::DateTime<chrono::Utc>, EvalError> {
    const MARGIN_SECS: i64 = 2 * 86_400;
    let out_of_range = || EvalError::new("time out of range");
    let secs = i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| out_of_range())?;
    let sub = u32::try_from(nanos.rem_euclid(1_000_000_000)).unwrap_or(0);
    let lo = chrono::DateTime::<chrono::Utc>::MIN_UTC.timestamp() + MARGIN_SECS;
    let hi = chrono::DateTime::<chrono::Utc>::MAX_UTC.timestamp() - MARGIN_SECS;
    if !(lo..=hi).contains(&secs) {
        return Err(out_of_range());
    }
    chrono::DateTime::from_timestamp(secs, sub).ok_or_else(out_of_range)
}

/// An RFC 3339 string in the system's local offset, at the unit's precision.
fn rfc3339(nanos: i128, u: Unit) -> Result<Value, EvalError> {
    let t = utc_from_nanos(nanos)?.with_timezone(&chrono::Local);
    let fmt = match u {
        Unit::Second => chrono::SecondsFormat::Secs,
        Unit::Milli => chrono::SecondsFormat::Millis,
        Unit::Micro => chrono::SecondsFormat::Micros,
        Unit::Nano => chrono::SecondsFormat::Nanos,
    };
    Ok(Value::from(t.to_rfc3339_opts(fmt, false)))
}

/// `Z`, `local`, `±hh[:mm][:ss]`, `±hh[mm][ss]`, or seconds as an integer.
fn offset_seconds(v: &Value) -> Result<i32, EvalError> {
    if let Value::Int(n) = v {
        return i32::try_from(*n).map_err(|_| EvalError::new("time zone offset out of range"));
    }
    let s = text(v)?;
    match s {
        "Z" | "z" => return Ok(0),
        "local" => return Ok(chrono::Local::now().offset().local_minus_utc()),
        _ => {}
    }
    let bad = || EvalError::new(format!("'{s}' is not a time zone offset"));
    let (sign, rest) = match s.as_bytes().first() {
        Some(b'+') => (1, &s[1..]),
        Some(b'-') => (-1, &s[1..]),
        _ => return Err(bad()),
    };
    let digits: String = rest.chars().filter(|c| *c != ':').collect();
    if !matches!(digits.len(), 2 | 4 | 6) || !digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(bad());
    }
    let part = |i: usize| {
        digits
            .get(i..i + 2)
            .and_then(|p| p.parse::<i32>().ok())
            .unwrap_or(0)
    };
    let (h, m, sec) = (part(0), part(2), part(4));
    if h > 23 || m > 59 || sec > 59 {
        return Err(bad());
    }
    Ok(sign * (h * 3600 + m * 60 + sec))
}

/// EMQX's date placeholders in chrono's: `%N` → nanoseconds, `%6N` → micro, `%3N` →
/// milli; the rest (`%Y %m %d %H %M %S %z %:z %::z`) are already chrono's.
fn chrono_format(fmt: &str) -> String {
    fmt.replace("%6N", "%6f")
        .replace("%3N", "%3f")
        .replace("%N", "%9f")
}

fn checked_items(fmt: &str) -> Result<Vec<chrono::format::Item<'_>>, EvalError> {
    let items: Vec<_> = chrono::format::StrftimeItems::new(fmt).collect();
    if items
        .iter()
        .any(|i| matches!(i, chrono::format::Item::Error))
    {
        return Err(EvalError::new(format!("invalid date format '{fmt}'")));
    }
    Ok(items)
}

fn format_time(t: &chrono::DateTime<chrono::FixedOffset>, fmt: &str) -> Result<String, EvalError> {
    let fmt = chrono_format(fmt);
    let items = checked_items(&fmt)?;
    // Not `to_string()`: a specifier chrono can parse but not format (`%#z`) makes the
    // formatter fail, and `to_string()` panics on a failing `Display`.
    let mut out = String::new();
    std::fmt::Write::write_fmt(
        &mut out,
        format_args!("{}", t.format_with_items(items.into_iter())),
    )
    .map_err(|_| {
        EvalError::new(format!(
            "date format '{fmt}' cannot be used to format a date"
        ))
    })?;
    Ok(out)
}

fn parse_time(fmt: &str, input: &str, offset: Option<i32>) -> Result<i128, EvalError> {
    let fmt = chrono_format(fmt);
    checked_items(&fmt)?;
    if let Ok(t) = chrono::DateTime::parse_from_str(input, &fmt) {
        return Ok(datetime_nanos(&t));
    }
    // No offset in the string: parse it naive and apply the given one.
    let naive_fmt = fmt.replace("%::z", "").replace("%:z", "").replace("%z", "");
    let naive = chrono::NaiveDateTime::parse_from_str(input, &naive_fmt)
        .map_err(|e| EvalError::new(format!("'{input}' does not match '{fmt}': {e}")))?;
    let off = chrono::FixedOffset::east_opt(offset.unwrap_or(0))
        .ok_or_else(|| EvalError::new("time zone offset out of range"))?;
    let t = naive
        .and_local_timezone(off)
        .single()
        .ok_or_else(|| EvalError::new("ambiguous local time"))?;
    Ok(datetime_nanos(&t))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_known_answers() {
        // RFC 1321 appendix A.5, plus EMQX's own example.
        assert_eq!(
            mqtt_core::hex_lower(&md5(b"")),
            "d41d8cd98f00b204e9800998ecf8427e"
        );
        assert_eq!(
            mqtt_core::hex_lower(&md5(b"abc")),
            "900150983cd24fb0d6963f7d28e17f72"
        );
        assert_eq!(
            mqtt_core::hex_lower(&md5(
                b"12345678901234567890123456789012345678901234567890123456789012345678901234567890"
            )),
            "57edf4a22be3c955ac49da2e2107b67a"
        );
        assert_eq!(
            mqtt_core::hex_lower(&md5(b"hello")),
            "5d41402abc4b2a76b9719d911017c592"
        );
    }

    #[test]
    fn erlang_replacements_translate() {
        assert_eq!(
            erlang_replacement(br"<\1>&$\g{12}\0").unwrap(),
            [
                RepPart::Lit(b"<".to_vec()),
                RepPart::Group(1),
                RepPart::Lit(b">".to_vec()),
                RepPart::Group(0),
                RepPart::Lit(b"$".to_vec()),
                RepPart::Group(12),
                RepPart::Lit(b"0".to_vec()),
            ]
        );
    }

    #[test]
    fn offsets_parse() {
        assert_eq!(offset_seconds(&Value::from("+08:00")).unwrap(), 28_800);
        assert_eq!(offset_seconds(&Value::from("-0130")).unwrap(), -5_400);
        assert_eq!(offset_seconds(&Value::from("+08:20:30")).unwrap(), 30_030);
        assert!(offset_seconds(&Value::from("8")).is_err());
    }

    #[test]
    fn function_names_are_unique() {
        let mut names = names();
        let n = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), n, "a function is registered twice");
    }
}
