//! Tokens of rule SQL, following EMQX's `rulesql` lexer:
//!
//! - `'single quotes'` are **string literals**; `"double quotes"` are **identifiers**
//!   (a column, a payload field, or a topic in `FROM`). This is EMQX's grammar, not a
//!   choice made here: `WHERE x = "abc"` compares `x` to a *field* named `abc`.
//!   A doubled quote does not end the literal, but it is not unescaped either: EMQX
//!   keeps it doubled (`'it''s'` is `it''s`) and strips every quote at either end
//!   (`'''x'''` is `x`).
//! - Keywords are case-insensitive; `div` and `mod` are the integer operators.
//! - `-- comment` runs to the end of the line.
//! - `[a..b]` (optionally signed ends) is one range token, as in EMQX.

use crate::ParseError;

/// A keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kw {
    Select,
    From,
    Where,
    As,
    And,
    Or,
    Not,
    In,
    Case,
    When,
    Then,
    Else,
    End,
    Foreach,
    Do,
    Incase,
}

impl Kw {
    fn parse(word: &str) -> Option<Self> {
        Some(match word.to_ascii_uppercase().as_str() {
            "SELECT" => Self::Select,
            "FROM" => Self::From,
            "WHERE" => Self::Where,
            "AS" => Self::As,
            "AND" => Self::And,
            "OR" => Self::Or,
            "NOT" => Self::Not,
            "IN" => Self::In,
            "CASE" => Self::Case,
            "WHEN" => Self::When,
            "THEN" => Self::Then,
            "ELSE" => Self::Else,
            "END" => Self::End,
            "FOREACH" => Self::Foreach,
            "DO" => Self::Do,
            "INCASE" => Self::Incase,
            _ => return None,
        })
    }
}

/// One token.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Tok {
    /// `'…'`
    Str(String),
    /// `"…"` — an identifier.
    QName(String),
    /// A bare identifier.
    Name(String),
    Int(i64),
    /// An integer literal beyond 64 bits (Erlang's `list_to_integer` has no width).
    BigInt(std::sync::Arc<num_bigint::BigInt>),
    Float(f64),
    Kw(Kw),
    /// `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `=~`
    Cmp(&'static str),
    Plus,
    Minus,
    Star,
    Slash,
    Div,
    Mod,
    Comma,
    Dot,
    LParen,
    RParen,
    LBracket,
    RBracket,
    /// `[a..b]`
    Range(i64, i64),
    Eof,
}

/// A token and the byte offset it starts at.
#[derive(Debug, Clone)]
pub(crate) struct Spanned {
    pub tok: Tok,
    pub start: usize,
    pub end: usize,
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_'
}

fn is_ident_continue(c: u8) -> bool {
    // EMQX also admits `$`, `@` and `~` after the first character.
    c.is_ascii_alphanumeric() || matches!(c, b'_' | b'$' | b'@' | b'~')
}

/// Tokenize `sql`.
// One arm per token shape: a flat scanner, not a refactor smell; `b`/`i`/`c` are
// the scanner's bytes, cursor and current byte.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
pub(crate) fn lex(sql: &str) -> Result<Vec<Spanned>, ParseError> {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        let start = i;
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'-' && b.get(i + 1) == Some(&b'-') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        let tok = match c {
            b'\'' | b'"' => {
                let quote = c;
                i += 1;
                let mut s = Vec::new();
                loop {
                    match b.get(i) {
                        None => {
                            return Err(ParseError::at(
                                sql,
                                start,
                                "unterminated quoted string or identifier",
                            ))
                        }
                        Some(&q) if q == quote => {
                            if b.get(i + 1) == Some(&quote) {
                                s.extend([quote, quote]);
                                i += 2;
                            } else {
                                i += 1;
                                break;
                            }
                        }
                        Some(&x) => {
                            s.push(x);
                            i += 1;
                        }
                    }
                }
                // EMQX's lexer keeps the token's text whole and its parser unquotes it
                // with `string:trim(Text, both, "'")`, which strips EVERY quote at either
                // end: a doubled quote stays doubled inside (`'it''s'` is `it''s`) and
                // is lost at an edge (`'''x'''` is `x`, `''''` is empty).
                let edge = |c: &u8| *c == quote;
                let lead = s.iter().take_while(|c| edge(c)).count();
                let trail = s[lead..].iter().rev().take_while(|c| edge(c)).count();
                s.truncate(s.len() - trail);
                s.drain(..lead);
                // The input is a &str and only ASCII quote bytes were removed, so this
                // cannot split a UTF-8 sequence.
                let s = String::from_utf8(s)
                    .map_err(|_| ParseError::at(sql, start, "quoted text is not valid UTF-8"))?;
                if quote == b'\'' {
                    Tok::Str(s)
                } else {
                    Tok::QName(s)
                }
            }
            b'[' => {
                if let Some((lo, hi, len)) = range_token(&b[i..]) {
                    i += len;
                    Tok::Range(lo, hi)
                } else {
                    i += 1;
                    Tok::LBracket
                }
            }
            b']' => {
                i += 1;
                Tok::RBracket
            }
            b'(' => {
                i += 1;
                Tok::LParen
            }
            b')' => {
                i += 1;
                Tok::RParen
            }
            b',' => {
                i += 1;
                Tok::Comma
            }
            b'.' => {
                i += 1;
                Tok::Dot
            }
            b'+' => {
                i += 1;
                Tok::Plus
            }
            b'-' => {
                i += 1;
                Tok::Minus
            }
            b'*' => {
                i += 1;
                Tok::Star
            }
            b'/' => {
                i += 1;
                Tok::Slash
            }
            b'=' => {
                if b.get(i + 1) == Some(&b'~') {
                    i += 2;
                    Tok::Cmp("=~")
                } else {
                    i += 1;
                    Tok::Cmp("=")
                }
            }
            b'!' if b.get(i + 1) == Some(&b'=') => {
                i += 2;
                Tok::Cmp("!=")
            }
            b'<' => match b.get(i + 1) {
                Some(b'>') => {
                    i += 2;
                    Tok::Cmp("<>")
                }
                Some(b'=') => {
                    i += 2;
                    Tok::Cmp("<=")
                }
                _ => {
                    i += 1;
                    Tok::Cmp("<")
                }
            },
            b'>' => {
                if b.get(i + 1) == Some(&b'=') {
                    i += 2;
                    Tok::Cmp(">=")
                } else {
                    i += 1;
                    Tok::Cmp(">")
                }
            }
            c if c.is_ascii_digit() => {
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                let mut float = false;
                if b.get(i) == Some(&b'.') && b.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    float = true;
                    i += 1;
                    while i < b.len() && b[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                if matches!(b.get(i), Some(b'e' | b'E')) {
                    let mut j = i + 1;
                    if matches!(b.get(j), Some(b'+' | b'-')) {
                        j += 1;
                    }
                    if b.get(j).is_some_and(u8::is_ascii_digit) {
                        float = true;
                        i = j;
                        while i < b.len() && b[i].is_ascii_digit() {
                            i += 1;
                        }
                    }
                }
                let text = &sql[start..i];
                // EMQX's lexer accepts an `f`/`d` suffix on approximate numbers.
                if float && matches!(b.get(i), Some(b'f' | b'F' | b'd' | b'D')) {
                    i += 1;
                }
                if float {
                    Tok::Float(
                        text.parse()
                            .ok()
                            .filter(|f: &f64| f.is_finite())
                            .ok_or_else(|| ParseError::at(sql, start, "malformed number"))?,
                    )
                } else {
                    match crate::num::parse_int(text.as_bytes()) {
                        Ok(Some(crate::value::Value::Int(n))) => Tok::Int(n),
                        Ok(Some(crate::value::Value::Big(n))) => Tok::BigInt(n),
                        Ok(_) => return Err(ParseError::at(sql, start, "malformed number")),
                        Err(e) => return Err(ParseError::at(sql, start, e.to_string())),
                    }
                }
            }
            c if is_ident_start(c) => {
                while i < b.len() && is_ident_continue(b[i]) {
                    i += 1;
                }
                let word = &sql[start..i];
                match word {
                    // Lower-case only, as in EMQX: `DIV` is an ordinary name there.
                    "div" => Tok::Div,
                    "mod" => Tok::Mod,
                    _ => Kw::parse(word).map_or_else(|| Tok::Name(word.to_string()), Tok::Kw),
                }
            }
            _ => {
                let ch = sql[start..].chars().next().unwrap_or('?');
                return Err(ParseError::at(
                    sql,
                    start,
                    format!("unexpected character '{ch}'"),
                ));
            }
        };
        out.push(Spanned { tok, start, end: i });
    }
    out.push(Spanned {
        tok: Tok::Eof,
        start: sql.len(),
        end: sql.len(),
    });
    Ok(out)
}

/// `[` optional-sign digits `..` optional-sign digits `]`, as one token.
fn range_token(b: &[u8]) -> Option<(i64, i64, usize)> {
    let mut i = 1;
    let num = |i: &mut usize| -> Option<i64> {
        let start = *i;
        if matches!(b.get(*i), Some(b'+' | b'-')) {
            *i += 1;
        }
        let digits = *i;
        while b.get(*i).is_some_and(u8::is_ascii_digit) {
            *i += 1;
        }
        if *i == digits {
            return None;
        }
        std::str::from_utf8(&b[start..*i]).ok()?.parse().ok()
    };
    let lo = num(&mut i)?;
    if b.get(i) != Some(&b'.') || b.get(i + 1) != Some(&b'.') {
        return None;
    }
    i += 2;
    let hi = num(&mut i)?;
    if b.get(i) != Some(&b']') {
        return None;
    }
    Some((lo, hi, i + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(s: &str) -> Vec<Tok> {
        lex(s).unwrap().into_iter().map(|t| t.tok).collect()
    }

    #[test]
    fn quotes_follow_emqx() {
        assert_eq!(
            toks(r#"'it''s' "t/#""#),
            [Tok::Str("it''s".into()), Tok::QName("t/#".into()), Tok::Eof]
        );
    }

    #[test]
    fn ranges_numbers_and_comments() {
        assert_eq!(
            toks("a[1..-1] 2.5e3 7 -- trailing\n x"),
            [
                Tok::Name("a".into()),
                Tok::Range(1, -1),
                Tok::Float(2500.0),
                Tok::Int(7),
                Tok::Name("x".into()),
                Tok::Eof
            ]
        );
    }

    #[test]
    fn operators() {
        assert_eq!(
            toks("<> != <= >= =~ = div mod DIV"),
            [
                Tok::Cmp("<>"),
                Tok::Cmp("!="),
                Tok::Cmp("<="),
                Tok::Cmp(">="),
                Tok::Cmp("=~"),
                Tok::Cmp("="),
                Tok::Div,
                Tok::Mod,
                Tok::Name("DIV".into()),
                Tok::Eof
            ]
        );
    }
}
