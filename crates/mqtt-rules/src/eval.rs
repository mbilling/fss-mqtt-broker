//! Statement and expression evaluation, following EMQX's `emqx_rule_runtime`:
//!
//! - **Order.** Every `SELECT` field, left to right, then the `WHERE`: a field that
//!   fails fails the rule even when the `WHERE` would have turned the message away. In a
//!   `FOREACH`, the collection, the `WHERE`, then per element the `INCASE` and the `DO`.
//! - **Lookup, by clause** (EMQX's `[Selected, Columns]` list versus a single merged map):
//!   - A `SELECT` field reads what the statement has selected so far, then the trigger's
//!     input fields; a path that is `undefined` in the first falls through to the second.
//!   - A `WHERE` reads one map, the input fields with the selected ones merged over them
//!     (`maps:merge(Columns, Selected)`): a selected top-level key hides the input's
//!     whole key, with no fall-through into it. `SELECT payload.x … WHERE payload.y = 1`
//!     is therefore false: the selected `payload` is `{"x": …}`.
//!   - An `INCASE` reads the same map with the element merged over it.
//!   - A `DO` field reads what the `DO` has selected so far, then falls through to that
//!     merged map (element included).
//! - **`payload.…` decodes JSON lazily**, once per message (shared by every rule the
//!   message matches). A payload that is not JSON makes a rule that reaches into it
//!   *fail* (counted), exactly as in EMQX; a rule that never looks inside the payload
//!   never pays for the decode.
//! - **Comparisons.** `undefined` compared with anything is false (two `undefined`s
//!   compare equal); a number against a string converts the string (a non-numeric string
//!   is an error); a boolean against a string compares their text; anything else uses
//!   Erlang term order.
//! - **Conditions pass only on boolean `true`.**

use std::cell::OnceCell;
use std::sync::Arc;

use crate::funcs::FnCtx;
use crate::parser::{ArithOp, CmpOp, Expr, Item, KeyPath, Seg, Statement};
use crate::value::{json_decode, Map, Value};
use crate::{EvalError, Input};

/// The most outputs one statement may produce for one trigger. A `FOREACH` over a
/// publisher-controlled array is otherwise an amplification factor the publisher picks.
pub const MAX_OUTPUTS_PER_TRIGGER: usize = 256;

/// The longest `[lo..hi]` range literal (it materialises an array).
const MAX_RANGE_LEN: i64 = 10_000;

/// The most elements one `FOREACH` iterates. The collection is usually a payload
/// array, and every element costs an `INCASE` evaluation even when it produces no
/// output, so the output cap alone does not bound the work.
pub const MAX_FOREACH_ELEMENTS: usize = 10_000;

/// Per-trigger evaluation state, shared by every rule the trigger matches.
pub struct EvalCtx<'a> {
    pub(crate) input: &'a dyn Input,
    payload_json: OnceCell<Result<Value, EvalError>>,
    all_fields: OnceCell<Map>,
    /// The rule being evaluated (EMQX's `metadata.rule_id`).
    pub(crate) rule_id: std::cell::RefCell<Arc<str>>,
    /// The regular expressions compiled from non-literal patterns during this message,
    /// most recent first, and what compiling each gave (see `funcs::regex`).
    pub(crate) regex_cache: RegexCache,
    /// Bytes this message's functions have built beyond their inputs so far (see
    /// `funcs::MAX_BUILT_BYTES`).
    pub(crate) built: std::cell::Cell<usize>,
    /// Bytes this message's effects carry so far (see [`crate::MAX_DERIVED_BYTES`]).
    pub(crate) derived: std::cell::Cell<usize>,
    /// Where name lookup stops falling through ([`lookup`]): frames from this index on,
    /// with the input after them, are one merged map. `usize::MAX` (a `SELECT`): every
    /// frame and the input fall through in turn. Set per clause by [`merged_from`].
    merged_from: std::cell::Cell<usize>,
}

/// Patterns and the result of compiling each, most recent first.
pub(crate) type RegexCache = std::cell::RefCell<Vec<(Vec<u8>, crate::funcs::Compiled)>>;

impl std::fmt::Debug for EvalCtx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvalCtx").finish_non_exhaustive()
    }
}

impl<'a> EvalCtx<'a> {
    /// A context over `input`.
    #[must_use]
    pub fn new(input: &'a dyn Input) -> Self {
        Self {
            input,
            payload_json: OnceCell::new(),
            all_fields: OnceCell::new(),
            rule_id: std::cell::RefCell::new(Arc::from("")),
            regex_cache: std::cell::RefCell::new(Vec::new()),
            built: std::cell::Cell::new(0),
            derived: std::cell::Cell::new(0),
            merged_from: std::cell::Cell::new(usize::MAX),
        }
    }

    /// The decoded payload (cached). An undecodable payload is an error for whichever
    /// rule asked.
    fn payload_json(&self) -> Result<Value, EvalError> {
        self.payload_json
            .get_or_init(|| match self.input.payload() {
                Some(p) => json_decode(p).map_err(|e| {
                    EvalError::new(format!(
                        "payload is not JSON, so payload.<field> is unreadable ({e})"
                    ))
                }),
                None => Ok(Value::Undefined),
            })
            .clone()
    }

    /// Every input field (`SELECT *`), plus EMQX's `metadata.rule_id`.
    fn all_fields(&self) -> Map {
        let mut m = self
            .all_fields
            .get_or_init(|| self.input.all_fields())
            .clone();
        let mut meta = Map::new();
        meta.insert("rule_id", Value::Str(self.rule_id.borrow().clone()));
        m.insert("metadata", Value::from(meta));
        m
    }

    /// One input field.
    fn input_field(&self, name: &str) -> Value {
        if name == "metadata" {
            let mut meta = Map::new();
            meta.insert("rule_id", Value::Str(self.rule_id.borrow().clone()));
            return Value::from(meta);
        }
        self.input.field(name)
    }
}

/// Scopes searched before the input fields, innermost first.
type Frames<'f> = &'f [&'f Map];

/// Run `stmt` against the trigger, appending each output to `out`. Returns whether
/// the `WHERE` matched (an empty `FOREACH` collection still counts as no result).
pub(crate) fn run(stmt: &Statement, ctx: &EvalCtx, out: &mut Vec<Map>) -> Result<(), EvalError> {
    if stmt.foreach {
        run_foreach(stmt, ctx, out)
    } else {
        let selected = select(&stmt.fields, ctx, &[])?;
        if merged_from(ctx, 0, || {
            condition(stmt.where_.as_ref(), ctx, &[&selected])
        })? {
            out.push(selected);
        }
        Ok(())
    }
}

/// Evaluate `f` with lookup treating frames from `from` on, and the input, as one merged
/// map ([`EvalCtx::merged_from`]), restoring the previous setting even if `f` fails.
fn merged_from<T>(ctx: &EvalCtx, from: usize, f: impl FnOnce() -> T) -> T {
    let before = ctx.merged_from.replace(from);
    let result = f();
    ctx.merged_from.set(before);
    result
}

fn run_foreach(stmt: &Statement, ctx: &EvalCtx, out: &mut Vec<Map>) -> Result<(), EvalError> {
    let mut selected = Map::new();
    // Aliases of the leading FOREACH expressions are visible to the later ones, so
    // `FOREACH payload.data as d, d.sensors as s` works (EMQX puts them into Columns).
    let mut aliases = Map::new();
    let mut collection = Vec::new();
    let mut item_name: Arc<str> = Arc::from("item");
    let n = stmt.fields.len();
    for (i, item) in stmt.fields.iter().enumerate() {
        let Item::Field {
            expr,
            key,
            explicit,
        } = item
        else {
            return Err(EvalError::new("FOREACH does not take *"));
        };
        let v = eval(expr, ctx, &[&selected, &aliases])?;
        if i + 1 < n {
            if *explicit {
                put(&mut aliases, key, v.clone());
            }
            put(&mut selected, key, v);
            continue;
        }
        if *explicit {
            if let [Seg::Key(k)] = key.as_slice() {
                item_name = k.clone();
            }
        }
        collection = match (&v, expr) {
            (Value::Array(a), _) => a.as_ref().clone(),
            // The bare `payload` is decoded for iteration (EMQX's ensure_list).
            (Value::Str(_) | Value::Bin(_), Expr::Path { head, segs })
                if segs.is_empty() && &**head == "payload" =>
            {
                match ctx.payload_json()? {
                    Value::Array(a) => a.as_ref().clone(),
                    _ => Vec::new(),
                }
            }
            _ => Vec::new(),
        };
        put(&mut selected, key, v);
    }
    if !merged_from(ctx, 0, || {
        condition(stmt.where_.as_ref(), ctx, &[&selected, &aliases])
    })? {
        return Ok(());
    }
    if collection.len() > MAX_FOREACH_ELEMENTS {
        return Err(EvalError::new(format!(
            "FOREACH over {} elements; at most {MAX_FOREACH_ELEMENTS} are iterated",
            collection.len()
        )));
    }
    for element in collection {
        let mut item = Map::with_capacity(1);
        item.insert(item_name.clone(), element);
        if !merged_from(ctx, 0, || {
            condition(stmt.incase.as_ref(), ctx, &[&item, &selected, &aliases])
        })? {
            continue;
        }
        if out.len() >= MAX_OUTPUTS_PER_TRIGGER {
            return Err(EvalError::new(format!(
                "FOREACH produced more than {MAX_OUTPUTS_PER_TRIGGER} outputs for one message"
            )));
        }
        let output = if stmt.do_fields.is_empty() {
            // No DO: the output is the whole scope, item included (EMQX).
            let mut all = ctx.all_fields();
            all.merge_from(&aliases);
            all.merge_from(&selected);
            all.merge_from(&item);
            all
        } else {
            // EMQX evaluates DO against `[DoSelected, ColumnsAndItem]`: the DO's own
            // fields fall through to one merged map of the input, the selection and the
            // element. `select` puts the DO's fields first, so the merged map starts at 1.
            merged_from(ctx, 1, || {
                select(&stmt.do_fields, ctx, &[&item, &selected, &aliases])
            })?
        };
        out.push(output);
    }
    Ok(())
}

/// Evaluate a `SELECT` (or `DO`) list into an output map.
fn select(items: &[Item], ctx: &EvalCtx, outer: Frames) -> Result<Map, EvalError> {
    let mut selected = Map::with_capacity(items.len());
    for item in items {
        match item {
            Item::Star => {
                selected.merge_from(&ctx.all_fields());
                for frame in outer.iter().rev() {
                    selected.merge_from(frame);
                }
            }
            Item::Field { expr, key, .. } => {
                let v = {
                    let mut frames: Vec<&Map> = Vec::with_capacity(outer.len() + 1);
                    frames.push(&selected);
                    frames.extend_from_slice(outer);
                    eval(expr, ctx, &frames)?
                };
                put(&mut selected, key, v);
            }
        }
    }
    Ok(selected)
}

/// A `WHERE` / `INCASE` / `WHEN`: absent passes; present passes only on `true`.
fn condition(cond: Option<&Expr>, ctx: &EvalCtx, frames: Frames) -> Result<bool, EvalError> {
    match cond {
        None => Ok(true),
        Some(e) => Ok(eval(e, ctx, frames)?.is_true()),
    }
}

/// Store `v` under `key` in `map`, creating (or JSON-decoding) intermediate maps.
pub(crate) fn put(map: &mut Map, key: &KeyPath, v: Value) {
    let Some((Seg::Key(head), rest)) = key.split_first() else {
        return;
    };
    if rest.is_empty() {
        map.insert(head.clone(), v);
        return;
    }
    let mut slot = map.remove(head).unwrap_or_default();
    put_value(&mut slot, rest, v);
    map.insert(head.clone(), slot);
}

fn put_value(target: &mut Value, path: &[Seg], v: Value) {
    let Some((seg, rest)) = path.split_first() else {
        *target = v;
        return;
    };
    match seg {
        Seg::Key(k) => {
            // A JSON string being extended (`SELECT payload, 1 AS payload.x`) is decoded
            // first, as EMQX does for the payload.
            if let Some(bytes) = target.as_bytes() {
                if let Ok(decoded @ Value::Map(_)) = json_decode(bytes) {
                    *target = decoded;
                }
            }
            if !matches!(target, Value::Map(_)) {
                *target = Value::from(Map::new());
            }
            if let Value::Map(m) = target {
                let m = Arc::make_mut(m);
                let mut child = m.remove(k).unwrap_or_default();
                put_value(&mut child, rest, v);
                m.insert(k.clone(), child);
            }
        }
        Seg::Index(i) => {
            if let Value::Array(a) = target {
                let a = Arc::make_mut(a);
                if let Some(at) = resolve_index(*i, a.len()) {
                    put_value(&mut a[at], rest, v);
                }
            }
        }
        // Aliases never carry computed indices (the parser does not produce them).
        Seg::IndexExpr(_) => {}
    }
}

/// A 1-based / negative-from-the-end index to a 0-based position.
pub(crate) fn resolve_index(i: i64, len: usize) -> Option<usize> {
    let len = i64::try_from(len).ok()?;
    let at = match i {
        0 => return None,
        i if i > 0 => i - 1,
        i => len + i,
    };
    (0..len)
        .contains(&at)
        .then(|| usize::try_from(at).ok())
        .flatten()
}

/// One step into a value. A string being stepped into is decoded as JSON first
/// (EMQX's `general_find`); one that is not JSON simply has no such field.
fn step(cur: &Value, seg: &Seg, ctx: &EvalCtx, frames: Frames) -> Result<Value, EvalError> {
    let decoded;
    let cur = match cur {
        Value::Str(_) | Value::Bin(_) => {
            decoded = json_decode(cur.as_bytes().unwrap_or_default()).unwrap_or_default();
            &decoded
        }
        other => other,
    };
    Ok(match seg {
        Seg::Key(k) => match cur {
            Value::Map(m) => m.get(k).cloned().unwrap_or_default(),
            _ => Value::Undefined,
        },
        Seg::Index(i) => index(cur, *i),
        Seg::IndexExpr(e) => match eval(e, ctx, frames)? {
            Value::Int(i) => index(cur, i),
            _ => Value::Undefined,
        },
    })
}

fn index(cur: &Value, i: i64) -> Value {
    match cur {
        Value::Array(a) => resolve_index(i, a.len())
            .map(|at| a[at].clone())
            .unwrap_or_default(),
        _ => Value::Undefined,
    }
}

fn walk(mut cur: Value, segs: &[Seg], ctx: &EvalCtx, frames: Frames) -> Result<Value, EvalError> {
    for seg in segs {
        if cur.is_undefined() {
            break;
        }
        cur = step(&cur, seg, ctx, frames)?;
    }
    Ok(cur)
}

/// Resolve `head.segs…` against the frames, then the input. A frame before
/// [`EvalCtx::merged_from`] is its own scope: a path that is `undefined` there falls
/// through. From that index on, the frames and the input are one merged map: the first
/// that has the top-level `head` decides, even if the rest of the path is `undefined`.
fn lookup(head: &str, segs: &[Seg], ctx: &EvalCtx, frames: Frames) -> Result<Value, EvalError> {
    let merged_from = ctx.merged_from.get();
    for (i, frame) in frames.iter().enumerate() {
        if let Some(v) = frame.get(head) {
            let v = walk(v.clone(), segs, ctx, frames)?;
            if i >= merged_from || !v.is_undefined() {
                return Ok(v);
            }
        }
    }
    if head == "payload" && !segs.is_empty() {
        return walk(ctx.payload_json()?, segs, ctx, frames);
    }
    walk(ctx.input_field(head), segs, ctx, frames)
}

/// Evaluate an expression.
pub(crate) fn eval(e: &Expr, ctx: &EvalCtx, frames: Frames) -> Result<Value, EvalError> {
    Ok(match e {
        Expr::Const(v) => v.clone(),
        Expr::Path { head, segs } => lookup(head, segs, ctx, frames)?,
        Expr::Get { base, segs } => walk(eval(base, ctx, frames)?, segs, ctx, frames)?,
        Expr::GetRange { base, lo, hi } => range_get(&eval(base, ctx, frames)?, *lo, *hi)?,
        Expr::RangeLit(lo, hi) => {
            if hi - lo > MAX_RANGE_LEN {
                return Err(EvalError::new(format!(
                    "range [{lo}..{hi}] is longer than {MAX_RANGE_LEN}"
                )));
            }
            Value::from((*lo..=*hi).map(Value::Int).collect::<Vec<_>>())
        }
        Expr::List(items) => Value::from(
            items
                .iter()
                .map(|x| eval(x, ctx, frames))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expr::Neg(x) => match eval(x, ctx, frames)? {
            Value::Int(n) => Value::Int(
                n.checked_neg()
                    .ok_or_else(|| EvalError::new("integer overflow"))?,
            ),
            Value::Float(f) => Value::Float(-f),
            v => return Err(EvalError::new(format!("cannot negate a {}", v.type_name()))),
        },
        Expr::Arith(op, l, r) => arith(*op, &eval(l, ctx, frames)?, &eval(r, ctx, frames)?)?,
        Expr::Cmp(op, l, r) => Value::Bool(compare(
            *op,
            &eval(l, ctx, frames)?,
            &eval(r, ctx, frames)?,
        )?),
        Expr::And(l, r) => {
            Value::Bool(eval(l, ctx, frames)?.is_true() && eval(r, ctx, frames)?.is_true())
        }
        Expr::Or(l, r) => {
            Value::Bool(eval(l, ctx, frames)?.is_true() || eval(r, ctx, frames)?.is_true())
        }
        // EMQX: NOT of a non-boolean is false, not an error.
        Expr::Not(x) => match eval(x, ctx, frames)? {
            Value::Bool(b) => Value::Bool(!b),
            _ => Value::Bool(false),
        },
        Expr::In {
            expr,
            list,
            negated,
        } => {
            let v = eval(expr, ctx, frames)?;
            let mut found = false;
            for x in list {
                if strict_eq(&v, &eval(x, ctx, frames)?) {
                    found = true;
                    break;
                }
            }
            Value::Bool(found != *negated)
        }
        Expr::Case {
            on,
            whens,
            otherwise,
        } => {
            let on = on.as_ref().map(|x| eval(x, ctx, frames)).transpose()?;
            for (when, then) in whens {
                let hit = match &on {
                    // `CASE x WHEN v` is an exact match (Erlang pattern match).
                    Some(v) => strict_eq(v, &eval(when, ctx, frames)?),
                    None => eval(when, ctx, frames)?.is_true(),
                };
                if hit {
                    return eval(then, ctx, frames);
                }
            }
            match otherwise {
                Some(x) => eval(x, ctx, frames)?,
                None => Value::Undefined,
            }
        }
        Expr::Call { func, args, regex } => {
            let args = args
                .iter()
                .map(|x| eval(x, ctx, frames))
                .collect::<Result<Vec<_>, _>>()?;
            let fcx = FnCtx {
                ctx,
                regex: regex.as_ref(),
            };
            (func.f)(&args, &fcx).map_err(|err| err.in_fn(func.name))?
        }
    })
}

/// Exact term equality (Erlang `=:=`): like `==` except an integer never equals a float.
fn strict_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(_), Value::Float(_)) | (Value::Float(_), Value::Int(_)) => false,
        _ => a.loose_eq(b),
    }
}

fn range_get(v: &Value, lo: i64, hi: i64) -> Result<Value, EvalError> {
    let Value::Array(a) = v else {
        return Err(EvalError::new(format!(
            "[{lo}..{hi}] needs an array, got a {}",
            v.type_name()
        )));
    };
    let len = i64::try_from(a.len()).unwrap_or(i64::MAX);
    let pos = |i: i64| -> Result<i64, EvalError> {
        match i {
            0 => Err(EvalError::new("array indices start at 1")),
            i if i > 0 => Ok(i),
            i => Ok(len + i + 1),
        }
    };
    let (b, e) = (pos(lo)?, pos(hi)?);
    let out: Vec<Value> = (b.max(1)..=e.min(len))
        .filter_map(|i| usize::try_from(i - 1).ok().and_then(|i| a.get(i).cloned()))
        .collect();
    Ok(Value::from(out))
}

/// A string read as a number for a comparison against one.
fn to_number(s: &[u8]) -> Result<Value, EvalError> {
    let text = std::str::from_utf8(s).unwrap_or_default().trim();
    if let Ok(n) = text.parse::<i64>() {
        return Ok(Value::Int(n));
    }
    text.parse::<f64>()
        .ok()
        .filter(|f| f.is_finite())
        .map(Value::Float)
        .ok_or_else(|| EvalError::new(format!("cannot compare a number with the string '{text}'")))
}

/// EMQX's `compare/3`.
pub(crate) fn compare(op: CmpOp, l: &Value, r: &Value) -> Result<bool, EvalError> {
    use std::cmp::Ordering::{Equal, Greater, Less};
    if op == CmpOp::TopicMatch {
        return Ok(match (l.as_str(), r.as_str()) {
            (Some(topic), Some(filter)) => mqtt_core::topic_matches(filter, topic),
            _ => false,
        });
    }
    let (l, r) = match (l, r) {
        (Value::Undefined, Value::Undefined) => (Value::Undefined, Value::Undefined),
        (Value::Undefined, _) | (_, Value::Undefined) => return Ok(false),
        (n, s) if n.is_number() && s.is_binary() => {
            (n.clone(), to_number(s.as_bytes().unwrap_or_default())?)
        }
        (s, n) if s.is_binary() && n.is_number() => {
            (to_number(s.as_bytes().unwrap_or_default())?, n.clone())
        }
        (Value::Bool(b), s) if s.is_binary() => {
            (Value::from(if *b { "true" } else { "false" }), s.clone())
        }
        (s, Value::Bool(b)) if s.is_binary() => {
            (s.clone(), Value::from(if *b { "true" } else { "false" }))
        }
        (Value::Null, s) if s.is_binary() => (Value::from("null"), s.clone()),
        (s, Value::Null) if s.is_binary() => (s.clone(), Value::from("null")),
        (a, b) => (a.clone(), b.clone()),
    };
    Ok(match op {
        CmpOp::Eq => l.loose_eq(&r),
        CmpOp::Ne => !l.loose_eq(&r),
        CmpOp::Lt => l.term_cmp(&r) == Less,
        CmpOp::Le => matches!(l.term_cmp(&r), Less | Equal),
        CmpOp::Gt => l.term_cmp(&r) == Greater,
        CmpOp::Ge => matches!(l.term_cmp(&r), Greater | Equal),
        CmpOp::TopicMatch => unreachable!("handled above"),
    })
}

fn overflow() -> EvalError {
    EvalError::new("integer overflow")
}

/// EMQX's arithmetic: `+` adds numbers or concatenates when either side is a string;
/// `/` always yields a float; `div` / `mod` are integer-only.
// `l`/`r` are the operands, `a`/`b` their integer forms, `x`/`y` their float forms.
#[allow(clippy::many_single_char_names)]
pub(crate) fn arith(op: ArithOp, l: &Value, r: &Value) -> Result<Value, EvalError> {
    if op == ArithOp::Add && (l.is_binary() || r.is_binary()) {
        let mut s = l.to_text()?;
        s.push_str(&r.to_text()?);
        return Ok(Value::from(s));
    }
    match (op, l, r) {
        (ArithOp::IntDiv | ArithOp::Mod, Value::Int(_), Value::Int(0))
        | (ArithOp::Div, _, Value::Int(0)) => return Err(EvalError::new("division by zero")),
        (ArithOp::Div, _, Value::Float(f)) if *f == 0.0 => {
            return Err(EvalError::new("division by zero"))
        }
        _ => {}
    }
    Ok(match (op, l, r) {
        (ArithOp::Add, Value::Int(a), Value::Int(b)) => {
            Value::Int(a.checked_add(*b).ok_or_else(overflow)?)
        }
        (ArithOp::Sub, Value::Int(a), Value::Int(b)) => {
            Value::Int(a.checked_sub(*b).ok_or_else(overflow)?)
        }
        (ArithOp::Mul, Value::Int(a), Value::Int(b)) => {
            Value::Int(a.checked_mul(*b).ok_or_else(overflow)?)
        }
        // Erlang `div` truncates toward zero and `rem` takes the dividend's sign —
        // exactly Rust's `/` and `%` on integers.
        (ArithOp::IntDiv, Value::Int(a), Value::Int(b)) => {
            Value::Int(a.checked_div(*b).ok_or_else(overflow)?)
        }
        (ArithOp::Mod, Value::Int(a), Value::Int(b)) => {
            Value::Int(a.checked_rem(*b).ok_or_else(overflow)?)
        }
        (ArithOp::IntDiv | ArithOp::Mod, _, _) => {
            return Err(EvalError::new(format!(
                "div and mod take integers, got {} and {}",
                l.type_name(),
                r.type_name()
            )))
        }
        (op, a, b) if a.is_number() && b.is_number() => {
            let (x, y) = (a.as_f64().unwrap_or(0.0), b.as_f64().unwrap_or(0.0));
            Value::float(match op {
                ArithOp::Add => x + y,
                ArithOp::Sub => x - y,
                ArithOp::Mul => x * y,
                _ => x / y,
            })?
        }
        _ => {
            return Err(EvalError::new(format!(
                "cannot apply arithmetic to {} and {}",
                l.type_name(),
                r.type_name()
            )))
        }
    })
}
