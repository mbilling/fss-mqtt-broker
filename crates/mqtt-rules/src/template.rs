//! `${…}` templates, as EMQX's actions use them.
//!
//! A placeholder names a path into the rule's **output** (what the SQL selected), not
//! into the raw input: `${clientid}` renders only if the SQL selected `clientid` (or
//! `*`). `${.}` is the whole output as JSON. A missing value renders as `undefined`,
//! EMQX's choice, which makes a template mistake visible in the republished message
//! rather than silently empty.
//!
//! Path syntax inside `${…}`: dot-separated keys, each bare or quoted (`'a.b'`,
//! `"a-b"`), with optional `[n]` indices — `${pub_props.'User-Property'.foo}`,
//! `${payload.list[1]}`. A string reached on the way is read as JSON.

use std::sync::Arc;

use crate::eval::resolve_index;
use crate::value::{json_decode, Map, Value};
use crate::EvalError;

/// One step of a template path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Key(Arc<str>),
    Index(i64),
}

#[derive(Debug, Clone)]
enum Part {
    Lit(Vec<u8>),
    Var(Vec<Step>),
    /// `${.}`
    This,
}

/// A parsed template.
#[derive(Debug, Clone)]
pub(crate) struct Template {
    parts: Vec<Part>,
}

impl Template {
    /// Parse `s`. An unterminated `${` is an error, not literal text.
    pub(crate) fn parse(s: &str) -> Result<Self, String> {
        let mut parts = Vec::new();
        let mut rest = s;
        while let Some(at) = rest.find("${") {
            if at > 0 {
                parts.push(Part::Lit(rest.as_bytes()[..at].to_vec()));
            }
            let after = &rest[at + 2..];
            let end = after
                .find('}')
                .ok_or_else(|| format!("unterminated '${{' in template \"{s}\""))?;
            parts.push(placeholder(&after[..end])?);
            rest = &after[end + 1..];
        }
        if !rest.is_empty() {
            parts.push(Part::Lit(rest.as_bytes().to_vec()));
        }
        Ok(Self { parts })
    }

    /// The template `${.}`.
    pub(crate) fn this() -> Self {
        Self {
            parts: vec![Part::This],
        }
    }

    /// If this template is exactly one placeholder, its path.
    pub(crate) fn sole_var(&self) -> Option<&[Step]> {
        match self.parts.as_slice() {
            [Part::Var(p)] => Some(p),
            _ => None,
        }
    }

    /// Whether this template has no placeholders.
    pub(crate) fn is_literal(&self) -> bool {
        self.parts.iter().all(|p| matches!(p, Part::Lit(_)))
    }

    /// The literal text before the first placeholder (all of it for a literal template,
    /// empty when it opens with one): what every rendering starts with.
    pub(crate) fn literal_prefix(&self) -> &str {
        match self.parts.first() {
            // Literal parts are cut from the template's text at `${` and `}`, both ASCII,
            // so they are always whole UTF-8.
            Some(Part::Lit(b)) => std::str::from_utf8(b).unwrap_or_default(),
            _ => "",
        }
    }

    /// Render against the rule output.
    pub(crate) fn render(&self, out: &Map) -> Result<Vec<u8>, EvalError> {
        let mut buf = Vec::new();
        for part in &self.parts {
            match part {
                Part::Lit(b) => buf.extend_from_slice(b),
                Part::This => buf.extend_from_slice(Value::from(out.clone()).to_json()?.as_bytes()),
                Part::Var(path) => lookup(out, path).render_into(&mut buf)?,
            }
        }
        Ok(buf)
    }
}

fn placeholder(inner: &str) -> Result<Part, String> {
    let inner = inner.trim();
    let inner = inner.strip_prefix('.').unwrap_or(inner);
    if inner.is_empty() {
        return Ok(Part::This);
    }
    Ok(Part::Var(parse_path(inner)?))
}

/// `a.'b.c'.d[2]` → `[a, "b.c", d, 2]`.
pub(crate) fn parse_path(s: &str) -> Result<Vec<Step>, String> {
    let mut steps = Vec::new();
    let b = s.as_bytes();
    let mut i = 0;
    loop {
        // One key: quoted, or bare up to `.` / `[`.
        match b.get(i) {
            Some(&q @ (b'\'' | b'"')) => {
                let close = s[i + 1..]
                    .find(q as char)
                    .ok_or_else(|| format!("unterminated quote in placeholder '{s}'"))?;
                steps.push(Step::Key(Arc::from(&s[i + 1..i + 1 + close])));
                i += close + 2;
            }
            Some(_) => {
                let len = s[i..].find(['.', '[']).unwrap_or(s.len() - i);
                if len == 0 {
                    return Err(format!("empty key in placeholder '{s}'"));
                }
                steps.push(Step::Key(Arc::from(&s[i..i + len])));
                i += len;
            }
            None => return Err(format!("empty key in placeholder '{s}'")),
        }
        while b.get(i) == Some(&b'[') {
            let close = s[i..]
                .find(']')
                .ok_or_else(|| format!("unterminated '[' in placeholder '{s}'"))?;
            let n = s[i + 1..i + close]
                .trim()
                .parse::<i64>()
                .map_err(|_| format!("index in placeholder '{s}' is not an integer"))?;
            steps.push(Step::Index(n));
            i += close + 1;
        }
        match b.get(i) {
            None => return Ok(steps),
            Some(b'.') => i += 1,
            Some(_) => return Err(format!("unexpected character in placeholder '{s}'")),
        }
    }
}

/// Look `path` up in `out`, reading strings met on the way as JSON.
pub(crate) fn lookup(out: &Map, path: &[Step]) -> Value {
    let Some((Step::Key(head), rest)) = path.split_first() else {
        return Value::Undefined;
    };
    let mut cur = out.get(head).cloned().unwrap_or_default();
    for step in rest {
        if let Value::Str(_) | Value::Bin(_) = cur {
            cur = json_decode(cur.as_bytes().unwrap_or_default()).unwrap_or_default();
        }
        cur = match (step, &cur) {
            (Step::Key(k), Value::Map(m)) => m.get(k).cloned().unwrap_or_default(),
            (Step::Index(n), Value::Array(a)) => resolve_index(*n, a.len())
                .map(|i| a[i].clone())
                .unwrap_or_default(),
            _ => Value::Undefined,
        };
    }
    cur
}

#[cfg(test)]
mod tests {
    use super::*;

    fn out() -> Map {
        let mut m = Map::new();
        m.insert("clientid", Value::from("c1"));
        m.insert("payload", Value::from(r#"{"x":{"y":[10,20]}}"#));
        m
    }

    #[test]
    fn renders_paths_and_missing_values() {
        let t = Template::parse("a/${clientid}/${payload.x.y[2]}/${nope}").unwrap();
        assert_eq!(t.render(&out()).unwrap(), b"a/c1/20/undefined");
    }

    #[test]
    fn this_is_the_whole_output() {
        let t = Template::parse("${.}").unwrap();
        assert_eq!(
            String::from_utf8(t.render(&out()).unwrap()).unwrap(),
            r#"{"clientid":"c1","payload":"{\"x\":{\"y\":[10,20]}}"}"#
        );
    }

    #[test]
    fn the_literal_prefix_is_the_text_before_the_first_placeholder() {
        let prefix = |s: &str| Template::parse(s).unwrap().literal_prefix().to_string();
        assert_eq!(prefix("$SYS/brokers/${node}/x"), "$SYS/brokers/");
        assert_eq!(prefix("a/b"), "a/b");
        assert_eq!(prefix("${t}/x"), "");
        assert_eq!(prefix("é/${t}"), "é/");
        assert_eq!(Template::this().literal_prefix(), "");
    }

    #[test]
    fn quoted_keys() {
        assert_eq!(
            parse_path("pub_props.'User-Property'.foo").unwrap(),
            vec![
                Step::Key("pub_props".into()),
                Step::Key("User-Property".into()),
                Step::Key("foo".into())
            ]
        );
        assert!(Template::parse("a/${b").is_err());
    }
}
