//! Rule SQL parser: tokens to a [`Statement`].
//!
//! The grammar is EMQX's (`rulesql.yrl`), parsed by recursive descent with the same
//! precedence table, lowest first:
//!
//! ```text
//! OR  <  AND  <  NOT  <  comparison / IN (non-associative)  <  + -  <  * / div mod  <  unary + -
//! ```
//!
//! It accepts a small, deliberate superset — every statement EMQX accepts parses to the
//! same meaning here; a few it rejects are accepted:
//!
//! - `AND` / `OR` / `NOT` anywhere an expression may appear (EMQX allows them only in
//!   `WHERE`/`INCASE`/`WHEN`), so `SELECT a > 1 and b < 2 AS ok` works;
//! - keywords as path segments after a dot (`payload.from`);
//! - field access on a computed value (`json_decode(payload).a`);
//! - identifiers starting with `_`, and `1e5`-style float literals.
//!
//! Function names are resolved here, so an unknown function or a wrong argument count
//! fails when the rules file is loaded, not on the first matching message.

use std::collections::HashMap;
use std::sync::Arc;

use crate::funcs::{self, Compiled, Func};
use crate::lexer::{lex, Kw, Spanned, Tok};
use crate::value::Value;
use crate::{ParseError, MAX_REGEX_LITERALS_PER_FILE, MAX_REGEX_LITERAL_BYTES_PER_FILE};

/// One step of a field path.
#[derive(Debug, Clone)]
pub(crate) enum Seg {
    /// `.name`, `."name"`, `.'name'` or `.3`
    Key(Arc<str>),
    /// `[n]` (1-based; negative counts from the end)
    Index(i64),
    /// `[expr]` — an index computed from another field.
    IndexExpr(Box<Expr>),
}

/// An arithmetic operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    IntDiv,
    Mod,
}

/// A comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    /// `=~`: topic matches filter.
    TopicMatch,
}

/// An expression.
#[derive(Debug, Clone)]
pub(crate) enum Expr {
    Const(Value),
    /// A field path rooted at a name: `clientid`, `payload.a.b[1]`, `"my-field"`.
    Path {
        head: Arc<str>,
        segs: Vec<Seg>,
    },
    /// Field access on a computed value: `json_decode(payload).a`.
    Get {
        base: Box<Expr>,
        segs: Vec<Seg>,
    },
    /// `path[lo..hi]`
    GetRange {
        base: Box<Expr>,
        lo: i64,
        hi: i64,
    },
    /// `[lo..hi]` as a value: the integers from `lo` to `hi`.
    RangeLit(i64, i64),
    List(Vec<Expr>),
    Neg(Box<Expr>),
    Arith(ArithOp, Box<Expr>, Box<Expr>),
    Cmp(CmpOp, Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    In {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Case {
        on: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        otherwise: Option<Box<Expr>>,
    },
    Call {
        func: &'static Func,
        args: Vec<Expr>,
        /// The pattern of a regex function, compiled at load time when it is a literal
        /// (or why it does not compile: the call fails, as in EMQX).
        regex: Option<Compiled>,
    },
}

/// Where a selected value is stored in the output.
pub(crate) type KeyPath = Vec<Seg>;

/// One item of a `SELECT` / `FOREACH` / `DO` list.
#[derive(Debug, Clone)]
pub(crate) enum Item {
    /// `*` — every input field.
    Star,
    Field {
        expr: Expr,
        key: KeyPath,
        /// Whether `key` was written with `AS` (or a bare alias) rather than derived.
        explicit: bool,
    },
}

/// A parsed rule statement.
#[derive(Debug, Clone)]
pub(crate) struct Statement {
    pub foreach: bool,
    pub fields: Vec<Item>,
    pub do_fields: Vec<Item>,
    pub incase: Option<Expr>,
    pub from: Vec<String>,
    pub where_: Option<Expr>,
}

/// Parse one statement. Returns it with any warnings worth showing the author. Its
/// literal regex patterns are compiled through `regexes`, which a whole rules file shares.
pub(crate) fn parse(
    sql: &str,
    regexes: &mut RegexPool,
) -> Result<(Statement, Vec<String>), ParseError> {
    let toks = lex(sql)?;
    let mut p = Parser {
        sql,
        toks,
        pos: 0,
        warnings: Vec::new(),
        nest: 0,
        regexes,
    };
    let stmt = p.statement()?;
    Ok((stmt, p.warnings))
}

/// The literal regex patterns one rules file has compiled (ADR 0084 D3), each once.
///
/// A pattern is bounded on its own (`funcs::compile_regex`), but a file is not: every
/// literal costs a compile, and the compiled patterns live as long as the rules. An
/// identical literal shares one compiled pattern wherever it appears, and a file may
/// hold at most [`MAX_REGEX_LITERALS_PER_FILE`] distinct ones; the next is refused
/// before it is compiled. A pattern that does not compile is kept as its error (the
/// rule loads, and the call fails when it runs, as in EMQX).
#[derive(Debug, Default)]
pub(crate) struct RegexPool {
    compiled: HashMap<String, Compiled>,
    /// The bytes of the patterns in `compiled`, together.
    bytes: usize,
}

impl RegexPool {
    /// `pattern` compiled (or its compile error): the file's earlier copy, or a new one
    /// while the budget lasts.
    pub(crate) fn get(&mut self, pattern: &str) -> Result<Compiled, String> {
        if let Some(re) = self.compiled.get(pattern) {
            return Ok(re.clone());
        }
        if self.compiled.len() >= MAX_REGEX_LITERALS_PER_FILE {
            return Err(format!(
                "more than {MAX_REGEX_LITERALS_PER_FILE} distinct regular expressions in one \
                 rules file (an identical pattern is compiled once and counts once)"
            ));
        }
        if self.bytes + pattern.len() > MAX_REGEX_LITERAL_BYTES_PER_FILE {
            return Err(format!(
                "more than {MAX_REGEX_LITERAL_BYTES_PER_FILE} bytes of distinct regular \
                 expressions in one rules file (an identical pattern is compiled once and \
                 counts once)"
            ));
        }
        let re = funcs::compile_regex(pattern.as_bytes());
        self.bytes += pattern.len();
        self.compiled.insert(pattern.to_string(), re.clone());
        Ok(re)
    }
}

/// The tallest expression a statement may hold. Evaluating an expression — and dropping
/// it — recurses once per level, so a taller tree (a 64 KiB `1+1+1+…`, or deep nesting)
/// would overflow a connection task's stack when a message arrives. It is refused at
/// load instead, with an error naming the place.
pub(crate) const MAX_EXPR_HEIGHT: usize = 256;

/// The deepest the parser itself recurses (parentheses and unary signs add no node).
const MAX_NESTING: usize = 64;

/// A parsed expression and its height.
type Parsed = Result<(Expr, usize), ParseError>;

struct Parser<'a> {
    sql: &'a str,
    toks: Vec<Spanned>,
    pos: usize,
    warnings: Vec<String>,
    /// Current recursion depth (see [`MAX_NESTING`]).
    nest: usize,
    /// Where literal regex patterns are compiled (see [`RegexPool`]).
    regexes: &'a mut RegexPool,
}

impl Parser<'_> {
    fn peek(&self) -> &Tok {
        &self.toks[self.pos].tok
    }

    fn peek_at(&self, n: usize) -> &Tok {
        let i = (self.pos + n).min(self.toks.len() - 1);
        &self.toks[i].tok
    }

    fn start(&self) -> usize {
        self.toks[self.pos].start
    }

    fn prev_end(&self) -> usize {
        self.toks[self.pos.saturating_sub(1)].end
    }

    fn bump(&mut self) -> Tok {
        let t = self.toks[self.pos].tok.clone();
        if self.pos < self.toks.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn eat(&mut self, t: &Tok) -> bool {
        if self.peek() == t {
            self.bump();
            true
        } else {
            false
        }
    }

    fn eat_kw(&mut self, k: Kw) -> bool {
        self.eat(&Tok::Kw(k))
    }

    fn err(&self, msg: impl Into<String>) -> ParseError {
        ParseError::at(self.sql, self.start(), msg)
    }

    fn expect(&mut self, t: &Tok, what: &str) -> Result<(), ParseError> {
        if self.eat(t) {
            Ok(())
        } else {
            Err(self.err(format!("expected {what}")))
        }
    }

    fn statement(&mut self) -> Result<Statement, ParseError> {
        let foreach = if self.eat_kw(Kw::Select) {
            false
        } else if self.eat_kw(Kw::Foreach) {
            true
        } else {
            return Err(self.err("a rule statement starts with SELECT or FOREACH"));
        };
        let fields = self.items()?;
        let (mut do_fields, mut incase) = (Vec::new(), None);
        if foreach {
            if self.eat_kw(Kw::Do) {
                do_fields = self.items()?;
            }
            if self.eat_kw(Kw::Incase) {
                incase = Some(self.expr()?);
            }
        }
        if !self.eat_kw(Kw::From) {
            return Err(self.err(
                "expected FROM followed by quoted topic filters or event topics, e.g. FROM \"t/#\"",
            ));
        }
        let mut from = Vec::new();
        loop {
            match self.bump() {
                Tok::Str(s) | Tok::QName(s) => from.push(s),
                _ => {
                    return Err(ParseError::at(
                        self.sql,
                        self.prev_end(),
                        "FROM takes quoted topic filters or event topics, e.g. FROM \"t/#\"",
                    ))
                }
            }
            if !self.eat(&Tok::Comma) {
                break;
            }
        }
        let where_ = if self.eat_kw(Kw::Where) {
            Some(self.expr()?)
        } else {
            None
        };
        if *self.peek() != Tok::Eof {
            return Err(self.err("unexpected text after the end of the statement"));
        }
        Ok(Statement {
            foreach,
            fields,
            do_fields,
            incase,
            from,
            where_,
        })
    }

    fn items(&mut self) -> Result<Vec<Item>, ParseError> {
        let mut items = Vec::new();
        loop {
            if self.eat(&Tok::Star) {
                items.push(Item::Star);
            } else {
                let start = self.start();
                let expr = self.expr()?;
                let text = self.sql[start..self.prev_end()].trim().to_string();
                let alias = if self.eat_kw(Kw::As) {
                    Some(self.key_path()?)
                } else if matches!(self.peek(), Tok::Name(_) | Tok::QName(_)) {
                    // EMQX allows an alias without AS: `SELECT payload.x x`.
                    Some(self.key_path()?)
                } else {
                    None
                };
                let explicit = alias.is_some();
                let key = alias.unwrap_or_else(|| implicit_key(&expr, text));
                items.push(Item::Field {
                    expr,
                    key,
                    explicit,
                });
            }
            if !self.eat(&Tok::Comma) {
                break;
            }
        }
        Ok(items)
    }

    /// An alias: `name`, `"name"`, `a.b.c`, `a[1]`.
    fn key_path(&mut self) -> Result<KeyPath, ParseError> {
        let (Tok::Name(head) | Tok::QName(head)) = self.bump() else {
            return Err(ParseError::at(
                self.sql,
                self.prev_end(),
                "expected an alias name after AS",
            ));
        };
        let mut path = vec![Seg::Key(head.into())];
        loop {
            if self.eat(&Tok::Dot) {
                path.push(Seg::Key(self.segment_name()?));
            } else if *self.peek() == Tok::LBracket {
                self.bump();
                let n = self.signed_int()?;
                self.expect(&Tok::RBracket, "']'")?;
                path.push(Seg::Index(n));
            } else {
                break;
            }
        }
        Ok(path)
    }

    fn signed_int(&mut self) -> Result<i64, ParseError> {
        let neg = if self.eat(&Tok::Minus) {
            true
        } else {
            self.eat(&Tok::Plus);
            false
        };
        match self.bump() {
            Tok::Int(n) => Ok(if neg { -n } else { n }),
            // An index past 64 bits is past any array's end: `undefined`, as in EMQX.
            Tok::BigInt(_) => Ok(if neg { i64::MIN } else { i64::MAX }),
            _ => Err(ParseError::at(
                self.sql,
                self.prev_end(),
                "expected an integer index",
            )),
        }
    }

    /// The name after a `.`: an identifier, a quoted name, a string, an integer, or a
    /// keyword used as a name.
    fn segment_name(&mut self) -> Result<Arc<str>, ParseError> {
        Ok(match self.bump() {
            Tok::Name(s) | Tok::QName(s) | Tok::Str(s) => s.into(),
            Tok::Int(n) => n.to_string().into(),
            Tok::BigInt(n) => n.to_string().into(),
            Tok::Div => "div".into(),
            Tok::Mod => "mod".into(),
            Tok::Kw(_) => {
                let t = &self.toks[self.pos - 1];
                self.sql[t.start..t.end].into()
            }
            _ => {
                return Err(ParseError::at(
                    self.sql,
                    self.prev_end(),
                    "expected a field name after '.'",
                ))
            }
        })
    }

    /// Parse an expression (for the statement level, which needs no height).
    fn expr(&mut self) -> Result<Expr, ParseError> {
        Ok(self.expr_h()?.0)
    }

    /// Parse an expression and return its height (see [`MAX_EXPR_HEIGHT`]).
    fn expr_h(&mut self) -> Parsed {
        self.nested(Self::or_expr)
    }

    /// Recurse one level deeper, refusing past [`MAX_NESTING`] (parentheses and unary
    /// signs recurse without adding a node, so height alone does not bound them).
    fn nested<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, ParseError>,
    ) -> Result<T, ParseError> {
        // The statement's own expression is the first level, so `>`: a rule may nest
        // MAX_NESTING levels of parentheses or signs inside it, as documented.
        if self.nest > MAX_NESTING {
            return Err(self.err(format!(
                "expression nests more than {MAX_NESTING} levels deep"
            )));
        }
        self.nest += 1;
        let r = f(self);
        self.nest -= 1;
        r
    }

    /// The height of a node over children of height `h`, refused past
    /// [`MAX_EXPR_HEIGHT`].
    fn node(&self, h: usize) -> Result<usize, ParseError> {
        let h = h + 1;
        if h > MAX_EXPR_HEIGHT {
            return Err(self.err(format!(
                "expression is more than {MAX_EXPR_HEIGHT} levels deep; split it \
                 (e.g. use IN (...) for a long OR of comparisons)"
            )));
        }
        Ok(h)
    }

    fn or_expr(&mut self) -> Parsed {
        let (mut l, mut h) = self.and_expr()?;
        while self.eat_kw(Kw::Or) {
            let (r, hr) = self.and_expr()?;
            h = self.node(h.max(hr))?;
            l = Expr::Or(Box::new(l), Box::new(r));
        }
        Ok((l, h))
    }

    fn and_expr(&mut self) -> Parsed {
        let (mut l, mut h) = self.not_expr()?;
        while self.eat_kw(Kw::And) {
            let (r, hr) = self.not_expr()?;
            h = self.node(h.max(hr))?;
            l = Expr::And(Box::new(l), Box::new(r));
        }
        Ok((l, h))
    }

    fn not_expr(&mut self) -> Parsed {
        if self.eat_kw(Kw::Not) {
            let (e, h) = self.nested(Self::not_expr)?;
            return Ok((Expr::Not(Box::new(e)), self.node(h)?));
        }
        self.cmp_expr()
    }

    fn cmp_expr(&mut self) -> Parsed {
        let (l, hl) = self.add_expr()?;
        let op = match self.peek() {
            Tok::Cmp(op) => *op,
            Tok::Kw(Kw::In) => {
                self.bump();
                return self.in_list(l, hl, false);
            }
            Tok::Kw(Kw::Not) if *self.peek_at(1) == Tok::Kw(Kw::In) => {
                self.bump();
                self.bump();
                return self.in_list(l, hl, true);
            }
            _ => return Ok((l, hl)),
        };
        self.bump();
        let (r, hr) = self.add_expr()?;
        let op = match op {
            "=" => CmpOp::Eq,
            "!=" | "<>" => CmpOp::Ne,
            "<" => CmpOp::Lt,
            "<=" => CmpOp::Le,
            ">" => CmpOp::Gt,
            ">=" => CmpOp::Ge,
            _ => CmpOp::TopicMatch,
        };
        if matches!(self.peek(), Tok::Cmp(_)) {
            return Err(
                self.err("comparisons do not chain; combine them with AND (e.g. a < b AND b < c)")
            );
        }
        let h = self.node(hl.max(hr))?;
        Ok((Expr::Cmp(op, Box::new(l), Box::new(r)), h))
    }

    /// `"x"` compared to something is almost always a string the author meant to write
    /// as `'x'`. It is still parsed EMQX's way — as a field — but said out loud. Called
    /// with the `QName` token just consumed.
    fn lint_quoted_literal(&mut self, name: &str) {
        let before = self.pos.checked_sub(2).map(|i| &self.toks[i].tok);
        let after = self.peek();
        let compared = matches!(before, Some(Tok::Cmp(_))) || matches!(after, Tok::Cmp(_));
        if compared && !matches!(after, Tok::Dot | Tok::LBracket) {
            self.warnings.push(format!(
                "\"{name}\" is a field name in rule SQL (double quotes quote identifiers, \
                 as in EMQX); write '{name}' for a string literal"
            ));
        }
    }

    fn in_list(&mut self, expr: Expr, h: usize, negated: bool) -> Parsed {
        self.expect(&Tok::LParen, "'(' after IN")?;
        let (first, mut hi) = self.expr_h()?;
        let mut list = vec![first];
        while self.eat(&Tok::Comma) {
            let (e, he) = self.expr_h()?;
            hi = hi.max(he);
            list.push(e);
        }
        self.expect(&Tok::RParen, "')' closing the IN list")?;
        let h = self.node(h.max(hi))?;
        Ok((
            Expr::In {
                expr: Box::new(expr),
                list,
                negated,
            },
            h,
        ))
    }

    fn add_expr(&mut self) -> Parsed {
        let (mut l, mut h) = self.mul_expr()?;
        loop {
            let op = match self.peek() {
                Tok::Plus => ArithOp::Add,
                Tok::Minus => ArithOp::Sub,
                _ => return Ok((l, h)),
            };
            self.bump();
            let (r, hr) = self.mul_expr()?;
            h = self.node(h.max(hr))?;
            l = Expr::Arith(op, Box::new(l), Box::new(r));
        }
    }

    fn mul_expr(&mut self) -> Parsed {
        let (mut l, mut h) = self.unary()?;
        loop {
            let op = match self.peek() {
                Tok::Star => ArithOp::Mul,
                Tok::Slash => ArithOp::Div,
                Tok::Div => ArithOp::IntDiv,
                Tok::Mod => ArithOp::Mod,
                _ => return Ok((l, h)),
            };
            self.bump();
            let (r, hr) = self.unary()?;
            h = self.node(h.max(hr))?;
            l = Expr::Arith(op, Box::new(l), Box::new(r));
        }
    }

    fn unary(&mut self) -> Parsed {
        if self.eat(&Tok::Minus) {
            let (e, h) = self.nested(Self::unary)?;
            return Ok(match e {
                // Negating never leaves the integer range (it only shrinks `-2^63`'s
                // literal back into an `i64`).
                Expr::Const(n @ (Value::Int(_) | Value::Big(_))) => {
                    (Expr::Const(crate::num::int_neg(&n).unwrap_or(n)), h)
                }
                Expr::Const(Value::Float(f)) => (Expr::Const(Value::Float(-f)), h),
                e => (Expr::Neg(Box::new(e)), self.node(h)?),
            });
        }
        if self.eat(&Tok::Plus) {
            return self.nested(Self::unary);
        }
        let (base, h) = self.primary()?;
        self.postfix(base, h)
    }

    fn postfix(&mut self, base: Expr, h: usize) -> Parsed {
        let mut segs = Vec::new();
        // The tallest index expression among the segments (0 when there is none).
        let mut hs = 0;
        loop {
            match self.peek() {
                Tok::Dot => {
                    self.bump();
                    segs.push(Seg::Key(self.segment_name()?));
                }
                Tok::LBracket => {
                    self.bump();
                    let (seg, h_seg) = self.index()?;
                    hs = hs.max(h_seg);
                    segs.push(seg);
                }
                Tok::Range(lo, hi) => {
                    let (lo, hi) = (*lo, *hi);
                    self.bump();
                    let h = if segs.is_empty() {
                        h
                    } else {
                        self.node(h.max(hs))?
                    };
                    let base = attach(base, std::mem::take(&mut segs));
                    return Ok((
                        Expr::GetRange {
                            base: Box::new(base),
                            lo,
                            hi,
                        },
                        self.node(h)?,
                    ));
                }
                _ => {
                    let h = if segs.is_empty() {
                        h
                    } else {
                        self.node(h.max(hs))?
                    };
                    return Ok((attach(base, segs), h));
                }
            }
        }
    }

    fn index(&mut self) -> Result<(Seg, usize), ParseError> {
        let seg = match self.peek() {
            Tok::Int(_) | Tok::BigInt(_) | Tok::Minus | Tok::Plus => {
                (Seg::Index(self.signed_int()?), 0)
            }
            _ => {
                let (e, h) = self.expr_h()?;
                (Seg::IndexExpr(Box::new(e)), h)
            }
        };
        self.expect(&Tok::RBracket, "']'")?;
        Ok(seg)
    }

    fn primary(&mut self) -> Parsed {
        let start = self.start();
        let leaf = |e: Expr| Ok((e, 1));
        match self.bump() {
            Tok::Str(s) => leaf(Expr::Const(Value::from(s))),
            Tok::Int(n) => leaf(Expr::Const(Value::Int(n))),
            Tok::BigInt(n) => leaf(Expr::Const(Value::Big(n))),
            Tok::Float(f) => leaf(Expr::Const(Value::Float(f))),
            Tok::Range(lo, hi) => leaf(Expr::RangeLit(lo, hi)),
            Tok::LParen => {
                let e = self.expr_h()?;
                self.expect(&Tok::RParen, "')'")?;
                Ok(e)
            }
            Tok::LBracket => {
                let mut list = Vec::new();
                let mut h = 0;
                if !self.eat(&Tok::RBracket) {
                    loop {
                        let (e, he) = self.expr_h()?;
                        h = h.max(he);
                        list.push(e);
                        if !self.eat(&Tok::Comma) {
                            break;
                        }
                    }
                    self.expect(&Tok::RBracket, "']' closing the list")?;
                }
                Ok((Expr::List(list), self.node(h)?))
            }
            Tok::Kw(Kw::Case) => self.case(),
            // EMQX's grammar: `div_or_mod '(' fun_args ')'` calls `div`/`mod` by name.
            Tok::Div | Tok::Mod if *self.peek() == Tok::LParen => {
                let t = &self.toks[self.pos - 1];
                let name = self.sql[t.start..t.end].to_string();
                self.bump();
                self.call(&name, start)
            }
            Tok::QName(s) => {
                self.lint_quoted_literal(&s);
                leaf(Expr::Path {
                    head: s.into(),
                    segs: Vec::new(),
                })
            }
            Tok::Name(s) => {
                if *self.peek() == Tok::LParen {
                    self.bump();
                    return self.call(&s, start);
                }
                if s.eq_ignore_ascii_case("true") {
                    return leaf(Expr::Const(Value::Bool(true)));
                }
                if s.eq_ignore_ascii_case("false") {
                    return leaf(Expr::Const(Value::Bool(false)));
                }
                leaf(Expr::Path {
                    head: s.into(),
                    segs: Vec::new(),
                })
            }
            _ => Err(ParseError::at(self.sql, start, "expected an expression")),
        }
    }

    fn case(&mut self) -> Parsed {
        let mut h = 0;
        let on = if *self.peek() == Tok::Kw(Kw::When) {
            None
        } else {
            let (e, he) = self.expr_h()?;
            h = he;
            Some(Box::new(e))
        };
        let mut whens = Vec::new();
        while self.eat_kw(Kw::When) {
            let (cond, hc) = self.expr_h()?;
            if !self.eat_kw(Kw::Then) {
                return Err(self.err("expected THEN"));
            }
            let (then, ht) = self.expr_h()?;
            h = h.max(hc).max(ht);
            whens.push((cond, then));
        }
        if whens.is_empty() {
            return Err(self.err("CASE needs at least one WHEN … THEN …"));
        }
        let otherwise = if self.eat_kw(Kw::Else) {
            let (e, he) = self.expr_h()?;
            h = h.max(he);
            Some(Box::new(e))
        } else {
            None
        };
        if !self.eat_kw(Kw::End) {
            return Err(self.err("expected END closing CASE"));
        }
        let h = self.node(h)?;
        Ok((
            Expr::Case {
                on,
                whens,
                otherwise,
            },
            h,
        ))
    }

    fn call(&mut self, name: &str, start: usize) -> Parsed {
        let mut args = Vec::new();
        let mut h = 0;
        if !self.eat(&Tok::RParen) {
            loop {
                let (e, he) = self.expr_h()?;
                h = h.max(he);
                args.push(e);
                if !self.eat(&Tok::Comma) {
                    break;
                }
            }
            self.expect(&Tok::RParen, "')' closing the argument list")?;
        }
        let Some(func) = funcs::lookup(name) else {
            return Err(ParseError::at(
                self.sql,
                start,
                format!(
                    "unknown function {name}() — see docs/RULES.md for the supported functions"
                ),
            ));
        };
        if args.len() < func.min || args.len() > func.max {
            let expected = if func.min == func.max {
                func.min.to_string()
            } else if func.max == usize::MAX {
                format!("at least {}", func.min)
            } else {
                format!("{}..={}", func.min, func.max)
            };
            return Err(ParseError::at(
                self.sql,
                start,
                format!(
                    "{name}() takes {expected} argument(s), {} given",
                    args.len()
                ),
            ));
        }
        let regex = match func.regex_arg.and_then(|i| args.get(i)) {
            Some(Expr::Const(Value::Str(pattern))) => {
                let re = self
                    .regexes
                    .get(pattern)
                    .map_err(|e| ParseError::at(self.sql, start, e))?;
                if let Err(e) = &re {
                    // EMQX accepts the rule and fails it on every message; so does this
                    // engine, but says so when the file loads.
                    self.warnings.push(format!(
                        "{name}(): {e}; every call fails the rule, as in EMQX"
                    ));
                }
                Some(re)
            }
            _ => None,
        };
        let h = self.node(h)?;
        Ok((Expr::Call { func, args, regex }, h))
    }
}

/// Apply postfix segments to a primary: extend a path, or wrap anything else.
fn attach(base: Expr, segs: Vec<Seg>) -> Expr {
    if segs.is_empty() {
        return base;
    }
    match base {
        Expr::Path { head, segs: mut s } => {
            s.extend(segs);
            Expr::Path { head, segs: s }
        }
        other => Expr::Get {
            base: Box::new(other),
            segs,
        },
    }
}

/// The output key of an unaliased field, following EMQX's `alias/2`: a path stores
/// under itself (`payload.x` → `{"payload":{"x":…}}`), a string constant under its
/// text, anything else under its source text.
fn implicit_key(expr: &Expr, text: String) -> KeyPath {
    match expr {
        Expr::Path { head, segs } if segs.iter().all(|s| !matches!(s, Seg::IndexExpr(_))) => {
            let mut k = vec![Seg::Key(head.clone())];
            k.extend(segs.iter().cloned());
            k
        }
        Expr::GetRange { base, .. } => implicit_key(base, text),
        Expr::Const(Value::Str(s)) => vec![Seg::Key(s.clone())],
        _ => vec![Seg::Key(text.into())],
    }
}
