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

use std::sync::Arc;

use regex::Regex;

use crate::funcs::{self, Func};
use crate::lexer::{lex, Kw, Spanned, Tok};
use crate::value::Value;
use crate::ParseError;

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
        /// The pattern of a regex function, compiled at load time when it is a literal.
        regex: Option<Arc<Regex>>,
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

/// Parse one statement. Returns it with any warnings worth showing the author.
pub(crate) fn parse(sql: &str) -> Result<(Statement, Vec<String>), ParseError> {
    let toks = lex(sql)?;
    let mut p = Parser {
        sql,
        toks,
        pos: 0,
        warnings: Vec::new(),
    };
    let stmt = p.statement()?;
    Ok((stmt, p.warnings))
}

struct Parser<'a> {
    sql: &'a str,
    toks: Vec<Spanned>,
    pos: usize,
    warnings: Vec<String>,
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

    fn expr(&mut self) -> Result<Expr, ParseError> {
        let mut l = self.and_expr()?;
        while self.eat_kw(Kw::Or) {
            let r = self.and_expr()?;
            l = Expr::Or(Box::new(l), Box::new(r));
        }
        Ok(l)
    }

    fn and_expr(&mut self) -> Result<Expr, ParseError> {
        let mut l = self.not_expr()?;
        while self.eat_kw(Kw::And) {
            let r = self.not_expr()?;
            l = Expr::And(Box::new(l), Box::new(r));
        }
        Ok(l)
    }

    fn not_expr(&mut self) -> Result<Expr, ParseError> {
        if self.eat_kw(Kw::Not) {
            return Ok(Expr::Not(Box::new(self.not_expr()?)));
        }
        self.cmp_expr()
    }

    fn cmp_expr(&mut self) -> Result<Expr, ParseError> {
        let l = self.add_expr()?;
        let op = match self.peek() {
            Tok::Cmp(op) => *op,
            Tok::Kw(Kw::In) => {
                self.bump();
                return self.in_list(l, false);
            }
            Tok::Kw(Kw::Not) if *self.peek_at(1) == Tok::Kw(Kw::In) => {
                self.bump();
                self.bump();
                return self.in_list(l, true);
            }
            _ => return Ok(l),
        };
        self.bump();
        let r = self.add_expr()?;
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
        Ok(Expr::Cmp(op, Box::new(l), Box::new(r)))
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

    fn in_list(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParseError> {
        self.expect(&Tok::LParen, "'(' after IN")?;
        let mut list = vec![self.expr()?];
        while self.eat(&Tok::Comma) {
            list.push(self.expr()?);
        }
        self.expect(&Tok::RParen, "')' closing the IN list")?;
        Ok(Expr::In {
            expr: Box::new(expr),
            list,
            negated,
        })
    }

    fn add_expr(&mut self) -> Result<Expr, ParseError> {
        let mut l = self.mul_expr()?;
        loop {
            let op = match self.peek() {
                Tok::Plus => ArithOp::Add,
                Tok::Minus => ArithOp::Sub,
                _ => return Ok(l),
            };
            self.bump();
            let r = self.mul_expr()?;
            l = Expr::Arith(op, Box::new(l), Box::new(r));
        }
    }

    fn mul_expr(&mut self) -> Result<Expr, ParseError> {
        let mut l = self.unary()?;
        loop {
            let op = match self.peek() {
                Tok::Star => ArithOp::Mul,
                Tok::Slash => ArithOp::Div,
                Tok::Div => ArithOp::IntDiv,
                Tok::Mod => ArithOp::Mod,
                _ => return Ok(l),
            };
            self.bump();
            let r = self.unary()?;
            l = Expr::Arith(op, Box::new(l), Box::new(r));
        }
    }

    fn unary(&mut self) -> Result<Expr, ParseError> {
        if self.eat(&Tok::Minus) {
            return Ok(match self.unary()? {
                Expr::Const(Value::Int(n)) => Expr::Const(Value::Int(-n)),
                Expr::Const(Value::Float(f)) => Expr::Const(Value::Float(-f)),
                e => Expr::Neg(Box::new(e)),
            });
        }
        if self.eat(&Tok::Plus) {
            return self.unary();
        }
        let base = self.primary()?;
        self.postfix(base)
    }

    fn postfix(&mut self, base: Expr) -> Result<Expr, ParseError> {
        let mut segs = Vec::new();
        loop {
            match self.peek() {
                Tok::Dot => {
                    self.bump();
                    segs.push(Seg::Key(self.segment_name()?));
                }
                Tok::LBracket => {
                    self.bump();
                    segs.push(self.index()?);
                }
                Tok::Range(lo, hi) => {
                    let (lo, hi) = (*lo, *hi);
                    self.bump();
                    let base = attach(base, std::mem::take(&mut segs));
                    return Ok(Expr::GetRange {
                        base: Box::new(base),
                        lo,
                        hi,
                    });
                }
                _ => return Ok(attach(base, segs)),
            }
        }
    }

    fn index(&mut self) -> Result<Seg, ParseError> {
        let seg = match self.peek() {
            Tok::Int(_) | Tok::Minus | Tok::Plus => Seg::Index(self.signed_int()?),
            _ => Seg::IndexExpr(Box::new(self.expr()?)),
        };
        self.expect(&Tok::RBracket, "']'")?;
        Ok(seg)
    }

    fn primary(&mut self) -> Result<Expr, ParseError> {
        let start = self.start();
        match self.bump() {
            Tok::Str(s) => Ok(Expr::Const(Value::from(s))),
            Tok::Int(n) => Ok(Expr::Const(Value::Int(n))),
            Tok::Float(f) => Ok(Expr::Const(Value::Float(f))),
            Tok::Range(lo, hi) => Ok(Expr::RangeLit(lo, hi)),
            Tok::LParen => {
                let e = self.expr()?;
                self.expect(&Tok::RParen, "')'")?;
                Ok(e)
            }
            Tok::LBracket => {
                let mut list = Vec::new();
                if !self.eat(&Tok::RBracket) {
                    list.push(self.expr()?);
                    while self.eat(&Tok::Comma) {
                        list.push(self.expr()?);
                    }
                    self.expect(&Tok::RBracket, "']' closing the list")?;
                }
                Ok(Expr::List(list))
            }
            Tok::Kw(Kw::Case) => self.case(),
            Tok::QName(s) => {
                self.lint_quoted_literal(&s);
                Ok(Expr::Path {
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
                    return Ok(Expr::Const(Value::Bool(true)));
                }
                if s.eq_ignore_ascii_case("false") {
                    return Ok(Expr::Const(Value::Bool(false)));
                }
                Ok(Expr::Path {
                    head: s.into(),
                    segs: Vec::new(),
                })
            }
            _ => Err(ParseError::at(self.sql, start, "expected an expression")),
        }
    }

    fn case(&mut self) -> Result<Expr, ParseError> {
        let on = if *self.peek() == Tok::Kw(Kw::When) {
            None
        } else {
            Some(Box::new(self.expr()?))
        };
        let mut whens = Vec::new();
        while self.eat_kw(Kw::When) {
            let cond = self.expr()?;
            if !self.eat_kw(Kw::Then) {
                return Err(self.err("expected THEN"));
            }
            whens.push((cond, self.expr()?));
        }
        if whens.is_empty() {
            return Err(self.err("CASE needs at least one WHEN … THEN …"));
        }
        let otherwise = if self.eat_kw(Kw::Else) {
            Some(Box::new(self.expr()?))
        } else {
            None
        };
        if !self.eat_kw(Kw::End) {
            return Err(self.err("expected END closing CASE"));
        }
        Ok(Expr::Case {
            on,
            whens,
            otherwise,
        })
    }

    fn call(&mut self, name: &str, start: usize) -> Result<Expr, ParseError> {
        let mut args = Vec::new();
        if !self.eat(&Tok::RParen) {
            args.push(self.expr()?);
            while self.eat(&Tok::Comma) {
                args.push(self.expr()?);
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
            Some(Expr::Const(Value::Str(pattern))) => Some(Arc::new(
                funcs::compile_regex(pattern).map_err(|e| ParseError::at(self.sql, start, e.0))?,
            )),
            _ => None,
        };
        Ok(Expr::Call { func, args, regex })
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
