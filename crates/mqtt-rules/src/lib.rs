//! The mqttd rule engine ([ADR 0083](../../../docs/adr/0083-rule-engine.md)): rules
//! written in **EMQX's rule SQL**, evaluated on the publish path, with EMQX's
//! `republish` and `console` actions.
//!
//! ```toml
//! [rules.high_temp]
//! sql = '''
//! SELECT payload.temp AS temp, clientid
//! FROM "sensors/+/data"
//! WHERE payload.temp > 30
//! '''
//! actions = [
//!   { function = "republish", args = { topic = "alerts/${clientid}", qos = 1, payload = "${.}" } },
//! ]
//! ```
//!
//! This crate is pure: it parses rules, matches a trigger against them and evaluates
//! the SQL, and hands back [`Effect`]s. It performs no I/O and holds no broker state —
//! the broker decides where evaluation runs (on the connection task that read the
//! publish, so it scales with connections and nodes) and how a republished message is
//! delivered (through the same acknowledgement gate as the message that caused it).
//!
//! The SQL dialect, the function set and every known difference from EMQX are
//! documented in `docs/RULES.md`.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use mqtt_core::filter_index::FilterIndex;
use mqtt_core::FilterKey;
use serde::Deserialize;

mod action;
mod eval;
mod funcs;
mod input;
mod lexer;
mod parser;
mod template;
mod value;

pub use action::{Effect, Republish};
pub use eval::{EvalCtx, MAX_OUTPUTS_PER_TRIGGER};
pub use funcs::names as function_names;
pub use input::{now_ms, pub_props, ClientInfo, EventInput, EventKind, PublishInput};
pub use value::{json_decode, Map, Value};

/// The most rules one file may define.
pub const MAX_RULES: usize = 1024;

/// The most actions one rule may run.
pub const MAX_ACTIONS_PER_RULE: usize = 16;

/// The most effects (republished messages and console lines) one trigger may produce
/// across every rule it matches. Bounds the amplification a single publish can cause.
pub const MAX_EFFECTS_PER_TRIGGER: usize = 1024;

/// What all of one message's effects may carry together, beyond four times the
/// message's own payload: the topics, payloads and properties of its derived messages,
/// and the text of its console lines. Without it a `FOREACH` fan-out, times up to 16
/// actions, times a template that repeats the payload, turns one small publish into a
/// gigabyte of derived messages. Past it, further actions fail; the message is still
/// routed.
pub const MAX_DERIVED_BYTES: usize = 4 << 20;

/// The longest rule statement accepted.
pub const MAX_SQL_BYTES: usize = 64 * 1024;

/// The most distinct literal regex patterns one rules file may compile (ADR 0084 D3);
/// an identical pattern is compiled once however often it appears, and counts once.
///
/// Each pattern is already bounded (1 MiB of compiled program, 1 MiB of lazy DFA), but
/// the file was not: a few thousand worst-case literals took seconds and gigabytes to
/// parse. Measured on the release build (4-core x86-64, 2026-10): a pattern at the
/// per-pattern limit (`(\w|\pN|\pS){47}`, `\w{50}`, `\pL{56}`, `.{2471}`) costs about
/// 7.5 ms and 1.05 MiB to compile, so 128 distinct ones took 0.9-1.1 s and 140 MiB, and
/// 96 take about 0.7 s and 107 MiB. 96 keeps a worst-case file under a second, and the
/// running set plus one candidate being checked or reloaded under 256 MiB. A file over
/// it fails to load, at boot, on reload, in `--check-rules` and in the admin API alike.
pub const MAX_REGEX_LITERALS_PER_FILE: usize = 96;

/// A rule statement that does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message} (line {line}, column {column}, near `{near}`)")]
pub struct ParseError {
    /// What is wrong.
    pub message: String,
    /// 1-based line.
    pub line: usize,
    /// 1-based column (in characters).
    pub column: usize,
    /// The text at the error.
    pub near: String,
}

/// The 1-based line and column (in characters) of byte `offset` in `text`.
fn line_column(text: &str, offset: usize) -> (usize, usize) {
    let before = &text[..offset];
    let line = before.matches('\n').count() + 1;
    let column = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
    (line, column)
}

impl ParseError {
    pub(crate) fn at(sql: &str, offset: usize, message: impl Into<String>) -> Self {
        let offset = offset.min(sql.len());
        let (line, column) = line_column(sql, offset);
        let near: String = sql[offset..].chars().take(24).collect();
        Self {
            message: message.into(),
            line,
            column,
            near: if near.is_empty() {
                "end of statement".into()
            } else {
                near
            },
        }
    }
}

/// A rule failing while it runs (a failure counted against the rule, never against
/// the message that triggered it).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct EvalError(pub(crate) String);

impl EvalError {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }

    pub(crate) fn in_fn(self, name: &str) -> Self {
        Self(format!("{name}(): {}", self.0))
    }
}

/// A rules file that cannot be loaded. Loading is all-or-nothing: one bad rule
/// rejects the file, and a reload keeps the running rules.
///
/// The text (`Display`) is what `mqttd --check-rules` and a rejected reload print; the
/// positions are for a caller that points at the place, such as the admin API.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LoadError {
    /// The file itself (unreadable, not TOML, an unknown key).
    #[error("rules file: {message}")]
    File {
        /// What is wrong.
        message: String,
        /// Where in the file text, in bytes, when the TOML parser said.
        span: Option<Range<usize>>,
    },
    /// One rule.
    #[error("rule `{id}`: {message}")]
    Rule {
        /// The rule's id.
        id: String,
        /// What is wrong with it.
        message: String,
        /// The 1-based line of a SQL error, counted from the start of the rule's `sql`.
        sql_line: Option<usize>,
        /// The 1-based column (in characters) of a SQL error.
        sql_column: Option<usize>,
    },
}

impl LoadError {
    fn file(message: impl Into<String>) -> Self {
        Self::File {
            message: message.into(),
            span: None,
        }
    }

    /// For a file error with a span, where it starts in `text` (the text that was
    /// parsed): the 1-based line and column (in characters).
    #[must_use]
    pub fn file_position(&self, text: &str) -> Option<(usize, usize)> {
        match self {
            Self::File {
                span: Some(span), ..
            } => text
                .is_char_boundary(span.start)
                .then(|| line_column(text, span.start)),
            _ => None,
        }
    }
}

/// The fields a rule reads from its trigger.
pub trait Input {
    /// One field by name; [`Value::Undefined`] when the trigger has no such field.
    fn field(&self, name: &str) -> Value;
    /// Every field (`SELECT *`).
    fn all_fields(&self) -> Map;
    /// The raw payload, for `payload.<field>` access.
    fn payload(&self) -> Option<&Bytes> {
        None
    }
    /// The trigger's MQTT 5 user properties, in wire order.
    fn user_properties(&self) -> &[(String, String)] {
        &[]
    }
}

/// How one rule fared for one trigger, reported as it happens (the broker turns these
/// into per-rule metrics, EMQX's `matched` / `passed` / `failed` / `no_result` /
/// `actions.*` counters).
#[derive(Debug, Clone, Copy)]
pub enum Outcome<'a> {
    /// The SQL ran and produced at least one output; its actions follow.
    Passed,
    /// The `FROM` matched but `WHERE` (or an empty `FOREACH`) produced nothing.
    NoResult,
    /// The SQL failed.
    Failed(&'a EvalError),
    /// One action ran.
    ActionOk,
    /// One action failed (a bad rendered topic, an invalid `qos`, …).
    ActionFailed(&'a EvalError),
}

/// One loaded rule.
#[derive(Debug)]
pub struct Rule {
    id: Arc<str>,
    description: String,
    enabled: bool,
    sql: String,
    stmt: parser::Statement,
    actions: Vec<action::Action>,
    topics: Vec<String>,
    events: Vec<EventKind>,
    /// When a failure of this rule was last reported loudly (unix seconds); see
    /// [`Rule::failure_report_due`].
    last_report: std::sync::atomic::AtomicU64,
}

impl Rule {
    /// Whether a failure of this rule at `now` (unix seconds) should be reported
    /// loudly: the first in each `interval` seconds, per rule. A rule that fails on
    /// every message is then logged once per interval however busy its topic, while a
    /// second failing rule is still logged too.
    #[must_use]
    pub fn failure_report_due(&self, now: u64, interval: u64) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        let last = self.last_report.load(Relaxed);
        (last == 0 || now >= last.saturating_add(interval))
            && self
                .last_report
                .compare_exchange(last, now.max(1), Relaxed, Relaxed)
                .is_ok()
    }

    /// The rule's id (its table name in the rules file).
    #[must_use]
    pub fn id(&self) -> &Arc<str> {
        &self.id
    }

    /// The rule's description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Whether the rule is enabled (`enable = false` rules are loaded and listed but
    /// never run).
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// The rule's SQL.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The topic filters in its `FROM`.
    #[must_use]
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// The events in its `FROM`.
    #[must_use]
    pub fn events(&self) -> &[EventKind] {
        &self.events
    }

    /// How many actions it runs per output.
    #[must_use]
    pub fn action_count(&self) -> usize {
        self.actions.len()
    }
}

/// A loaded rules file.
#[derive(Debug, Default)]
pub struct RuleSet {
    rules: Vec<Rule>,
    topic_index: FilterIndex,
    by_filter: HashMap<FilterKey, Vec<usize>>,
    by_event: [Vec<usize>; 4],
    digest: String,
}

/// A successfully loaded rules file and what the author should be told about it.
#[derive(Debug)]
pub struct Loaded {
    /// The rules.
    pub rules: RuleSet,
    /// Non-fatal findings (a `"double-quoted"` literal, an ignored argument).
    pub warnings: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSchema {
    #[serde(default)]
    rules: BTreeMap<String, RuleSchema>,
}

fn yes() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleSchema {
    sql: String,
    #[serde(default)]
    actions: Vec<toml::Value>,
    #[serde(default = "yes")]
    enable: bool,
    #[serde(default)]
    description: String,
}

fn valid_id(id: &str) -> bool {
    let mut chars = id.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && id.len() <= 64
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// A statement, its `FROM` list split by source, and its warnings.
struct Compiled {
    stmt: parser::Statement,
    topics: Vec<String>,
    events: Vec<EventKind>,
    warnings: Vec<String>,
}

/// Why a statement does not compile, and where in it when the parser said.
struct CompileError {
    message: String,
    /// 1-based (line, column) in the statement.
    at: Option<(usize, usize)>,
}

impl From<String> for CompileError {
    fn from(message: String) -> Self {
        Self { message, at: None }
    }
}

impl From<&str> for CompileError {
    fn from(message: &str) -> Self {
        message.to_string().into()
    }
}

/// Compile one statement and its `FROM` list, its literal regex patterns through
/// `regexes`.
fn compile(sql: &str, regexes: &mut parser::RegexPool) -> Result<Compiled, CompileError> {
    if sql.len() > MAX_SQL_BYTES {
        return Err(format!("sql is longer than {MAX_SQL_BYTES} bytes").into());
    }
    let (stmt, warnings) = parser::parse(sql, regexes).map_err(|e| CompileError {
        message: e.to_string(),
        at: Some((e.line, e.column)),
    })?;
    if stmt.foreach && stmt.fields.iter().any(|i| matches!(i, parser::Item::Star)) {
        return Err("FOREACH takes an array expression, not *".into());
    }
    let (mut topics, mut events) = (Vec::new(), Vec::new());
    for from in &stmt.from {
        if from.starts_with("$events/") {
            let kind = EventKind::from_topic(from).ok_or_else(|| {
                CompileError::from(format!(
                    "\"{from}\" is not a supported event (supported: $events/client/connected, \
                     $events/client/disconnected, $events/session/subscribed, \
                     $events/session/unsubscribed)"
                ))
            })?;
            if !events.contains(&kind) {
                events.push(kind);
            }
        } else if from.starts_with("$bridges/") {
            return Err(
                format!("\"{from}\": mqttd has no data bridges to select from (ADR 0083)").into(),
            );
        } else if mqtt_core::parse_shared(from).is_some() || from.starts_with("$share/") {
            return Err(format!(
                "\"{from}\": a rule selects messages by topic filter; $share groups are for subscribers"
            )
            .into());
        } else if !mqtt_core::valid_filter(from) {
            return Err(format!("\"{from}\" is not a valid topic filter").into());
        } else if !topics.contains(from) {
            topics.push(from.clone());
        }
    }
    Ok(Compiled {
        stmt,
        topics,
        events,
        warnings,
    })
}

impl RuleSet {
    /// No rules.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Load a rules file's text. All-or-nothing.
    pub fn parse(text: &str) -> Result<Loaded, LoadError> {
        let file: FileSchema = toml::from_str(text).map_err(|e| LoadError::File {
            message: e.to_string(),
            span: e.span(),
        })?;
        if file.rules.len() > MAX_RULES {
            return Err(LoadError::file(format!(
                "{} rules is more than the {MAX_RULES} a file may define",
                file.rules.len()
            )));
        }
        let mut set = RuleSet {
            digest: mqtt_core::hex_lower(
                aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, text.as_bytes()).as_ref(),
            ),
            ..RuleSet::default()
        };
        let mut warnings = Vec::new();
        // One pool for the whole file: the regex budget is file-wide (ADR 0084 D3).
        let mut regexes = parser::RegexPool::default();
        for (id, r) in file.rules {
            let fail = |e: CompileError| LoadError::Rule {
                id: id.clone(),
                message: e.message,
                sql_line: e.at.map(|(line, _)| line),
                sql_column: e.at.map(|(_, column)| column),
            };
            if !valid_id(&id) {
                return Err(fail(
                    "a rule id is a letter or `_` followed by up to 63 letters, digits, `_` or `-`"
                        .into(),
                ));
            }
            if r.actions.len() > MAX_ACTIONS_PER_RULE {
                return Err(fail(
                    format!(
                        "{} actions is more than the {MAX_ACTIONS_PER_RULE} a rule may run",
                        r.actions.len()
                    )
                    .into(),
                ));
            }
            let Compiled {
                stmt,
                topics,
                events,
                warnings: w,
            } = compile(&r.sql, &mut regexes).map_err(fail)?;
            warnings.extend(w.into_iter().map(|w| format!("rule `{id}`: {w}")));
            let mut actions = Vec::with_capacity(r.actions.len());
            let mut aw = Vec::new();
            for a in &r.actions {
                actions.push(action::parse_action(a, &mut aw).map_err(|e| fail(e.into()))?);
            }
            warnings.extend(aw.into_iter().map(|w| format!("rule `{id}`: {w}")));
            set.rules.push(Rule {
                id: Arc::from(id.as_str()),
                description: r.description,
                enabled: r.enable,
                sql: r.sql,
                stmt,
                actions,
                topics,
                events,
                last_report: std::sync::atomic::AtomicU64::new(0),
            });
        }
        for (i, rule) in set.rules.iter().enumerate() {
            if !rule.enabled {
                continue;
            }
            for t in &rule.topics {
                let key: FilterKey = Arc::from(t.as_str());
                set.topic_index.insert(&key);
                set.by_filter.entry(key).or_default().push(i);
            }
            for e in &rule.events {
                set.by_event[e.index()].push(i);
            }
        }
        Ok(Loaded {
            rules: set,
            warnings,
        })
    }

    /// Load a rules file from disk.
    pub fn load(path: &std::path::Path) -> Result<Loaded, LoadError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| LoadError::file(format!("{}: {e}", path.display())))?;
        Self::parse(&text)
    }

    /// Every loaded rule (enabled or not), in id order.
    #[must_use]
    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    /// How many rules are loaded.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Whether no rule is loaded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// SHA-256 of the file text this set was loaded from (empty for [`empty`](Self::empty)):
    /// the value operators compare across nodes to see that every node runs the same rules.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Whether any enabled rule selects messages. The publish path checks this first,
    /// so a broker without message rules pays one branch per publish.
    #[must_use]
    pub fn has_message_rules(&self) -> bool {
        !self.topic_index.is_empty()
    }

    /// Whether any enabled rule selects `kind`.
    #[must_use]
    pub fn wants_event(&self, kind: EventKind) -> bool {
        !self.by_event[kind.index()].is_empty()
    }

    /// Evaluate every enabled rule whose `FROM` matches the message's topic, in id
    /// order. `report` sees each rule's [`Outcome`]s; each effect is appended to `out`
    /// with the id of the rule that produced it.
    pub fn on_publish(
        &self,
        input: &PublishInput<'_>,
        report: &mut dyn FnMut(&Rule, Outcome<'_>),
        out: &mut Vec<(Arc<str>, Effect)>,
    ) {
        if self.topic_index.is_empty() {
            return;
        }
        let mut hits: Vec<usize> = Vec::new();
        self.topic_index.for_each_matching(input.topic, |f| {
            if let Some(rules) = self.by_filter.get(f) {
                hits.extend_from_slice(rules);
            }
        });
        if hits.is_empty() {
            return;
        }
        // A rule whose FROM lists several matching filters fires once.
        hits.sort_unstable();
        hits.dedup();
        let ctx = EvalCtx::new(input);
        for i in hits {
            apply(&self.rules[i], &ctx, report, out);
        }
    }

    /// Evaluate every enabled rule selecting this event.
    pub fn on_event(
        &self,
        input: &EventInput,
        report: &mut dyn FnMut(&Rule, Outcome<'_>),
        out: &mut Vec<(Arc<str>, Effect)>,
    ) {
        let hits = &self.by_event[input.kind().index()];
        if hits.is_empty() {
            return;
        }
        let ctx = EvalCtx::new(input);
        for &i in hits {
            apply(&self.rules[i], &ctx, report, out);
        }
    }
}

/// Run one rule against a trigger: the SQL, then each action per output.
fn apply(
    rule: &Rule,
    ctx: &EvalCtx<'_>,
    report: &mut dyn FnMut(&Rule, Outcome<'_>),
    out: &mut Vec<(Arc<str>, Effect)>,
) {
    *ctx.rule_id.borrow_mut() = rule.id.clone();
    let mut outputs = Vec::new();
    if let Err(e) = eval::run(&rule.stmt, ctx, &mut outputs) {
        report(rule, Outcome::Failed(&e));
        return;
    }
    if outputs.is_empty() {
        report(rule, Outcome::NoResult);
        return;
    }
    report(rule, Outcome::Passed);
    for output in &outputs {
        for a in &rule.actions {
            if out.len() >= MAX_EFFECTS_PER_TRIGGER {
                let e = EvalError::new(format!(
                    "this message already produced {MAX_EFFECTS_PER_TRIGGER} effects; \
                     the rest are dropped"
                ));
                report(rule, Outcome::ActionFailed(&e));
                continue;
            }
            let effect = match a {
                action::Action::Console => {
                    Value::from(output.clone()).to_json().map(Effect::Console)
                }
                action::Action::Republish(spec) => {
                    spec.render(output, ctx.input).map(Effect::Republish)
                }
            };
            match effect.and_then(|e| charge_derived(ctx, e)) {
                Ok(e) => {
                    report(rule, Outcome::ActionOk);
                    out.push((rule.id.clone(), e));
                }
                Err(e) => report(rule, Outcome::ActionFailed(&e)),
            }
        }
    }
}

/// Charge an effect to the message's [`MAX_DERIVED_BYTES`] budget, refusing it past.
fn charge_derived(ctx: &EvalCtx<'_>, effect: Effect) -> Result<Effect, EvalError> {
    let bytes = match &effect {
        Effect::Republish(r) => r.topic.len() + r.payload.len() + r.app.accounted_bytes(),
        Effect::Console(line) => line.len(),
    };
    let own = ctx.input.payload().map_or(0, Bytes::len);
    let limit = MAX_DERIVED_BYTES.saturating_add(own.saturating_mul(4));
    let total = ctx.derived.get().saturating_add(bytes);
    if total > limit {
        return Err(EvalError::new(format!(
            "this message's effects would carry more than {limit} bytes \
             ({MAX_DERIVED_BYTES} plus four times its payload); the rest are dropped"
        )));
    }
    ctx.derived.set(total);
    Ok(effect)
}

/// What one statement's `FROM` selects: its topic filters and its events.
pub fn statement_sources(sql: &str) -> Result<(Vec<String>, Vec<EventKind>), String> {
    compile_alone(sql).map(|c| (c.topics, c.events))
}

/// [`compile`] for a statement on its own, with a regex budget of its own.
fn compile_alone(sql: &str) -> Result<Compiled, String> {
    compile(sql, &mut parser::RegexPool::default()).map_err(|e| e.message)
}

/// Run one statement against one input and return its outputs as JSON — the
/// `mqttd --rule-test` backend, EMQX's "SQL test".
///
/// An input the broker would never have run the rule on is reported as such rather
/// than evaluated: a message whose topic no `FROM` filter matches, a message given to a
/// statement that selects only events, or an event its `FROM` does not name.
pub fn test_sql(sql: &str, input: &dyn Input) -> Result<Vec<String>, String> {
    let Compiled {
        stmt,
        topics,
        events,
        ..
    } = compile_alone(sql)?;
    let event = input.field("event");
    let event = event.as_str().unwrap_or("message.publish");
    if event == "message.publish" {
        if topics.is_empty() {
            return Err(format!(
                "the statement selects only events ({}), so it never runs on a message; \
                 simulate one of them instead",
                events
                    .iter()
                    .map(|k| k.event_name())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(topic) = input.field("topic").as_str() {
            if !topics.iter().any(|f| mqtt_core::topic_matches(f, topic)) {
                return Err(format!(
                    "topic \"{topic}\" matches none of the FROM filters ({})",
                    topics.join(", ")
                ));
            }
        }
    } else if !events.iter().any(|k| k.event_name() == event) {
        return Err(format!(
            "the statement's FROM does not select the {event} event"
        ));
    }
    let ctx = EvalCtx::new(input);
    *ctx.rule_id.borrow_mut() = Arc::from("test");
    let mut outputs = Vec::new();
    eval::run(&stmt, &ctx, &mut outputs).map_err(|e| e.to_string())?;
    outputs
        .into_iter()
        .map(|o| Value::from(o).to_json().map_err(|e| e.to_string()))
        .collect()
}

/// Validate one statement without running it; returns its warnings.
pub fn check_sql(sql: &str) -> Result<Vec<String>, String> {
    compile_alone(sql).map(|c| c.warnings)
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_emqx_examples;
