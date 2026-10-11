//! Replaying a sequence of messages through rules that keep state
//! ([ADR 0085](../../../docs/adr/0085-rule-state-store.md) D11): what
//! `mqttd --rule-test --sequence <file>` runs, and what a test calls to assert a
//! sequence.
//!
//! A rule with state cannot be judged from one message. A sequence is a list of
//! messages and events, each at a time of the sequence's own; a [`Replay`] evaluates
//! them in order through one statement or through a whole rules file, and reports for
//! each step what the rules produced and what the step changed in the store.
//!
//! # What a replay is
//!
//! * **One store for the whole sequence**, empty at the start and dropped at the end
//!   ([`Store::scratch_at`]): never the store a running broker's rules keep their
//!   state in (D11: no test touches the running store).
//! * **One dictionary per client id** in the sequence, for EMQX's `proc_dict_*`: in
//!   the broker a dictionary is a connection's, and a sequence has no connections, so
//!   every step of one client id is taken to be one connection of it.
//! * **A clock the sequence drives.** The store, `now_timestamp()`, `now_rfc3339()`,
//!   `timestamp` and `publish_received_at` all read the step's time; nothing reads the
//!   wall clock, so a replay prints the same thing every time. The sequence starts at
//!   [`START_MS`].
//! * **The rules run as the broker runs them**: every enabled rule whose `FROM`
//!   selects the message, in id order, state calls in EMQX's evaluation order; a
//!   republished message re-enters the rules on the same dictionary and within the
//!   same per-message budget, depth first, as on a connection's task.
//! * **One node.** A namespace declared with another `consistency` than `local` has no
//!   other member to replicate to here, and behaves as `local`.
//!
//! # What waits for the expiry event
//!
//! A key past its expiry reads as absent at once, and a step reports the keys the
//! clock has passed since the step before. Raising `$events/state/expired` for them
//! (D7) is ADR 0085 T4's; `expire`, below, is where it attaches.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::io::{BufRead, Read as _};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use bytes::Bytes;
use mqtt_rules::state::{Consistency, Dictionary, KV_STORE_NAMESPACE};
use mqtt_rules::{
    Budget, ClientInfo, Effect, EventInput, EventKind, Functions, Input, Outcome, PublishInput,
    Recursion, RuleSet, SqlTest, StateSwitch,
};
use serde::Deserialize;

use crate::rule_state::Store;

/// The most steps a sequence has.
pub const MAX_STEPS: usize = 10_000;

/// The longest line of a sequence file, in bytes.
pub const MAX_LINE_BYTES: usize = 1 << 20;

/// When a sequence starts, in milliseconds since the Unix epoch:
/// 2025-01-01T00:00:00Z. A step's time is this plus its `at`.
pub const START_MS: u64 = 1_735_689_600_000;

/// The furthest from its start a sequence may run: a hundred years.
pub const MAX_AT_MS: u64 = 100 * 365 * 24 * 60 * 60 * 1000;

/// `node`, as `mqttd --rule-test` has it.
const NODE: &str = "rule-test";

/// What a line of a sequence that leaves a field out gets: `mqttd --rule-test`'s
/// options, with its defaults.
#[derive(Debug, Clone)]
pub struct Defaults {
    /// `--topic` (`t/1`).
    pub topic: String,
    /// `--payload` (`{}`).
    pub payload: Bytes,
    /// `--clientid` (`test-client`).
    pub clientid: String,
    /// `--username` (none).
    pub username: Option<String>,
    /// `--qos` (0).
    pub qos: u8,
    /// `--event`: every step that names no event is this one.
    pub event: Option<EventKind>,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            topic: "t/1".to_string(),
            payload: Bytes::from_static(b"{}"),
            clientid: "test-client".to_string(),
            username: None,
            qos: 0,
            event: None,
        }
    }
}

/// One step of a sequence.
#[derive(Debug, Clone)]
pub struct Step {
    /// The line of the file it was read from, from 1.
    pub line: usize,
    /// Its time, in milliseconds from the sequence's start.
    pub at_ms: u64,
    /// What is sent; `None` for a step that only moves the clock.
    pub send: Option<Trigger>,
}

/// What a step sends: a publish, or a sample of an event.
#[derive(Debug, Clone)]
pub struct Trigger {
    /// The event, for a step that raises one: a sample of it, as `mqttd --rule-test
    /// --event` builds it. `None`: a publish (or, for a statement that selects only
    /// events, the first event it names).
    pub event: Option<EventKind>,
    /// The topic.
    pub topic: String,
    /// The payload.
    pub payload: Bytes,
    /// 0, 1 or 2.
    pub qos: u8,
    /// The RETAIN flag.
    pub retain: bool,
    /// The publisher's client id, or the client the event is about.
    pub clientid: String,
    /// Its username.
    pub username: Option<String>,
}

/// One line of a sequence file.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Line<'a> {
    at: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    topic: Option<String>,
    #[serde(borrow)]
    payload: Option<&'a serde_json::value::RawValue>,
    qos: Option<u64>,
    retain: Option<bool>,
    clientid: Option<String>,
    username: Option<String>,
    event: Option<String>,
}

/// A time in a sequence: a number of seconds, or text such as `"90s"`, in
/// milliseconds.
fn duration_ms(key: &str, v: &serde_json::Value) -> Result<u64, String> {
    let bad = || {
        format!(
            "`{key}` is a number of seconds, or text such as \"250ms\", \"90s\", \"5m\", \
             \"1h\" or \"2d\", not {v}"
        )
    };
    let ms = match v {
        serde_json::Value::Number(n) => n
            .as_f64()
            .and_then(|secs| std::time::Duration::try_from_secs_f64(secs).ok())
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .ok_or_else(bad)?,
        serde_json::Value::String(s) => {
            let s = s.trim();
            let digits = s.len() - s.trim_start_matches(|c: char| c.is_ascii_digit()).len();
            let n: u64 = s[..digits].parse().map_err(|_| bad())?;
            let unit = match &s[digits..] {
                "ms" => 1,
                "s" => 1000,
                "m" => 60 * 1000,
                "h" => 60 * 60 * 1000,
                "d" => 24 * 60 * 60 * 1000,
                _ => return Err(bad()),
            };
            n.saturating_mul(unit)
        }
        _ => return Err(bad()),
    };
    if ms > MAX_AT_MS {
        return Err(format!(
            "`{key}` {v} is further than a sequence may run (a hundred years)"
        ));
    }
    Ok(ms)
}

/// Read a sequence: one JSON object per line.
///
/// | Key | |
/// |---|---|
/// | `at` | the step's time, from the sequence's start: a number of seconds (`1.5`), or text (`"250ms"`, `"90s"`, `"5m"`, `"1h"`, `"2d"`). Never before the step before it |
/// | `after` | instead of `at`: how long after the step before it |
/// | `topic`, `payload`, `qos`, `retain`, `clientid`, `username` | the publish. A `payload` that is a JSON string is those bytes; any other JSON value is its text as the line has it |
/// | `event` | instead of a publish, a sample of this event (`client.connected`, …), as `mqttd --rule-test --event` builds it |
///
/// A line with neither `at` nor `after` happens at the time of the step before it (the
/// first one at the start). A line with no `topic`, no `payload` and no `event` sends
/// nothing: it only moves the clock. What a line leaves out is `defaults`'. Blank
/// lines, and lines that start with `#`, are skipped.
///
/// Bounded: [`MAX_STEPS`] steps, [`MAX_LINE_BYTES`] a line. `Err` names the line.
pub fn parse_sequence(mut reader: impl BufRead, defaults: &Defaults) -> Result<Vec<Step>, String> {
    let mut steps: Vec<Step> = Vec::new();
    let mut buf = Vec::new();
    let mut line = 0usize;
    let limit = u64::try_from(MAX_LINE_BYTES).unwrap_or(u64::MAX) + 2;
    loop {
        buf.clear();
        line += 1;
        let read = (&mut reader)
            .take(limit)
            .read_until(b'\n', &mut buf)
            .map_err(|e| format!("line {line}: {e}"))?;
        if read == 0 {
            break;
        }
        let at_line = |e: String| format!("line {line}: {e}");
        let text = buf.strip_suffix(b"\n").unwrap_or(&buf);
        let text = text.strip_suffix(b"\r").unwrap_or(text);
        if text.len() > MAX_LINE_BYTES {
            return Err(at_line(format!(
                "longer than the {MAX_LINE_BYTES} bytes a line of a sequence may be"
            )));
        }
        let text = std::str::from_utf8(text)
            .map_err(|_| at_line("not UTF-8".to_string()))?
            .trim();
        if text.is_empty() || text.starts_with('#') {
            continue;
        }
        if steps.len() == MAX_STEPS {
            return Err(at_line(format!("a sequence has at most {MAX_STEPS} steps")));
        }
        let before = steps.last().map_or(0, |s| s.at_ms);
        let step = parse_line(text, line, before, defaults).map_err(at_line)?;
        steps.push(step);
    }
    if steps.is_empty() {
        return Err("the sequence has no step (one JSON object per line)".to_string());
    }
    Ok(steps)
}

fn parse_line(text: &str, line: usize, before: u64, defaults: &Defaults) -> Result<Step, String> {
    let l: Line<'_> = serde_json::from_str(text)
        // The line is the file's, which the caller names; serde's is always 1.
        .map_err(|e| {
            format!(
                "not a step (a JSON object on one line): {}",
                e.to_string().replace(" at line 1 column ", " at column ")
            )
        })?;
    let at_ms = match (&l.at, &l.after) {
        (Some(_), Some(_)) => {
            return Err("a step has `at` or `after`, not both".to_string());
        }
        (Some(at), None) => {
            let at_ms = duration_ms("at", at)?;
            if at_ms < before {
                return Err(format!(
                    "`at` {at} is before the step before it ({}): a sequence's clock does not \
                     go back",
                    clock(before)
                ));
            }
            at_ms
        }
        (None, Some(after)) => {
            let at_ms = before.saturating_add(duration_ms("after", after)?);
            if at_ms > MAX_AT_MS {
                return Err(format!(
                    "`after` {after} takes the sequence further than it may run (a hundred \
                     years)"
                ));
            }
            at_ms
        }
        (None, None) => before,
    };
    if l.topic.is_none() && l.payload.is_none() && l.event.is_none() {
        if l.qos.is_some() || l.retain.is_some() || l.clientid.is_some() || l.username.is_some() {
            return Err(
                "a step with no `topic`, no `payload` and no `event` sends nothing and only \
                 moves the clock: it takes `at` or `after` and nothing else"
                    .to_string(),
            );
        }
        return Ok(Step {
            line,
            at_ms,
            send: None,
        });
    }
    let event = match &l.event {
        None => defaults.event,
        Some(name) => Some(EventKind::parse(name).ok_or_else(|| {
            let names: Vec<&str> = EventKind::ALL.iter().map(|k| k.event_name()).collect();
            format!("`event` is one of {}, not {name:?}", names.join(", "))
        })?),
    };
    let topic = l.topic.unwrap_or_else(|| defaults.topic.clone());
    if !mqtt_core::valid_topic_name(&topic) {
        return Err(format!("`topic` {topic:?} is not a valid topic name"));
    }
    let payload = match l.payload {
        None => defaults.payload.clone(),
        Some(raw) if raw.get().starts_with('"') => {
            let text: String = serde_json::from_str(raw.get())
                .map_err(|e| format!("`payload` is not a JSON string: {e}"))?;
            Bytes::from(text)
        }
        Some(raw) => Bytes::copy_from_slice(raw.get().as_bytes()),
    };
    let qos = match l.qos {
        None => defaults.qos,
        Some(0) => 0,
        Some(1) => 1,
        Some(2) => 2,
        Some(other) => return Err(format!("`qos` is 0, 1 or 2, not {other}")),
    };
    let clientid = l.clientid.unwrap_or_else(|| defaults.clientid.clone());
    if clientid.is_empty() {
        return Err("`clientid` is empty".to_string());
    }
    Ok(Step {
        line,
        at_ms,
        send: Some(Trigger {
            event,
            topic,
            payload,
            qos,
            retain: l.retain.unwrap_or(false),
            clientid,
            username: l.username.or_else(|| defaults.username.clone()),
        }),
    })
}

/// A time on the sequence's clock as the report prints it: seconds from its start.
fn clock(at_ms: u64) -> String {
    if at_ms.is_multiple_of(1000) {
        format!("+{}s", at_ms / 1000)
    } else {
        format!("+{}.{:03}s", at_ms / 1000, at_ms % 1000)
    }
}

/// Bytes as a terminal may show them: text, with every control character as its
/// escape, so a payload cannot clear the screen or break the report's lines.
fn printable(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_control() {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
    out
}

/// A key of the store as the report prints it: as JSON. A `kv_*` key is a string;
/// one of EMQX's table is any value.
fn key_text(namespace: &str, key: &[u8]) -> String {
    let term = (namespace == KV_STORE_NAMESPACE)
        .then(|| mqtt_rules::state::key_term(key))
        .flatten()
        .and_then(|term| term.to_json().ok());
    let text = term.unwrap_or_else(|| {
        serde_json::to_string(&String::from_utf8_lossy(key)).unwrap_or_default()
    });
    printable(text.as_bytes())
}

/// What a key holds, as the report prints it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Shown {
    /// The value `kv_get` would read, as JSON.
    value: String,
    kind: &'static str,
    /// On the store's clock; `0`: never.
    expires_ms: u64,
}

impl std::fmt::Display for Shown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}", self.value, self.kind)?;
        if self.expires_ms != 0 {
            write!(
                f,
                ", expires {}",
                clock(self.expires_ms.saturating_sub(START_MS))
            )?;
        }
        f.write_str(")")
    }
}

/// What the store holds, as the replay last saw it: by namespace, then by key.
type Model = BTreeMap<(Arc<str>, Arc<[u8]>), Shown>;

/// The keys whose expiry the sequence's clock has passed by `now_ms`, in order of
/// expiry, taken out of `model`: from this moment each reads as absent (the store
/// removes it when a call finds it, or when its namespace needs room).
///
/// **Where ADR 0085 T4 attaches.** D7 raises `$events/state/expired` for each of
/// these, in this order, on the node's own task, and a rule on that event may write
/// state (the key that expired included). In a replay that is: for each key returned
/// here, at the key's `expires_ms` on the sequence's clock and before the step's own
/// message, evaluate the rules that select the event (on a dictionary of its own, as
/// the store's expiry task has) and report what they produced. Until the event
/// exists, a replay only reports the keys.
fn expire(model: &mut Model, now_ms: u64) -> Vec<(Arc<str>, Arc<[u8]>, u64)> {
    let mut expired: Vec<_> = model
        .iter()
        .filter(|(_, shown)| shown.expires_ms != 0 && shown.expires_ms <= now_ms)
        .map(|((namespace, key), shown)| (namespace.clone(), key.clone(), shown.expires_ms))
        .collect();
    for (namespace, key, _) in &expired {
        model.remove(&(namespace.clone(), key.clone()));
    }
    expired.sort_by(|a, b| (a.2, &a.0, &a.1).cmp(&(b.2, &b.0, &b.1)));
    expired
}

/// What one step did.
#[derive(Debug, Clone)]
pub struct StepReport {
    /// The step, from 1.
    pub number: usize,
    /// The line of the sequence file it was read from.
    pub line: usize,
    /// Its time, in milliseconds from the sequence's start.
    pub at_ms: u64,
    /// What was sent, as the report's heading shows it.
    pub sent: String,
    /// What the rules produced, one entry a line: for a statement its outputs as
    /// JSON; for a rules file each rule that ran, its result and what its actions
    /// rendered, then the same for each republished message that re-entered the
    /// rules.
    pub results: Vec<String>,
    /// What the step changed in the store, one key a line, by namespace and key:
    /// `+` a new key, `~` a changed value or expiry, `-` a key that is gone. The keys
    /// that expired since the step before come first.
    pub state: Vec<String>,
    /// Whether a rule the step ran failed, or one of its actions did.
    pub failed: bool,
    /// Whether any rule ran on what was sent (`false` for a step that sends nothing).
    pub selected: bool,
}

/// What a replay did, step by step.
#[derive(Debug, Clone)]
pub struct Report {
    /// The steps, in order.
    pub steps: Vec<StepReport>,
    /// What the store held after the last step, one key a line, by namespace and key.
    pub store: Vec<String>,
    /// When the last step happened, in milliseconds from the sequence's start.
    pub end_ms: u64,
    /// What the reader should know about the rules as a replay runs them (a namespace
    /// that would be replicated in a cluster).
    pub notes: Vec<String>,
}

impl Report {
    /// How many steps had a rule or an action fail.
    #[must_use]
    pub fn failed(&self) -> usize {
        self.steps.iter().filter(|s| s.failed).count()
    }

    /// Whether any rule ran on any step.
    #[must_use]
    pub fn selected(&self) -> bool {
        self.steps.iter().any(|s| s.selected)
    }

    /// The report as `mqttd --rule-test --sequence` prints it; with `store`, followed
    /// by what the store held at the end (`--show-state`).
    #[must_use]
    pub fn text(&self, store: bool) -> String {
        let mut out = String::new();
        for note in &self.notes {
            let _ = writeln!(out, "note: {note}");
        }
        for step in &self.steps {
            let _ = writeln!(out, "#{} {} {}", step.number, clock(step.at_ms), step.sent);
            for line in step.results.iter().chain(&step.state) {
                let _ = writeln!(out, "  {line}");
            }
        }
        if store {
            let _ = writeln!(out, "state at the end ({}):", clock(self.end_ms));
            if self.store.is_empty() {
                out.push_str("  (empty)\n");
            }
            for line in &self.store {
                let _ = writeln!(out, "  {line}");
            }
        }
        out
    }
}

/// The rules a replay runs.
#[derive(Debug)]
enum Rules {
    Statement(Box<SqlTest>),
    File(Box<RuleSet>),
}

/// One statement, or one rules file, ready to replay one sequence on a store of its
/// own.
#[derive(Debug)]
pub struct Replay {
    rules: Rules,
    /// `None` where rule state is switched off: no rule that keeps state loads.
    store: Option<Arc<Store>>,
    /// The sequence's time, which the store reads.
    now_ms: Arc<AtomicU64>,
    warnings: Vec<String>,
}

/// `functions` with a store for one replay in place of whichever it carried, and that
/// store's clock. Where rule state is switched off (`rules.state_max_bytes = 0`) it
/// stays off.
fn on_scratch(functions: &Functions) -> (Functions, Option<Arc<Store>>, Arc<AtomicU64>) {
    let now_ms = Arc::new(AtomicU64::new(START_MS));
    let max_bytes = match &functions.state {
        StateSwitch::Off => return (functions.clone(), None, now_ms),
        StateSwitch::On(store) => store.max_bytes(),
        StateSwitch::None => mqtt_config::Rules::default().state_max_bytes,
    };
    let store = Store::scratch_at(max_bytes, now_ms.clone());
    (
        functions.clone().with_state(store.clone()),
        Some(store),
        now_ms,
    )
}

impl Replay {
    /// A replay of one statement, as `mqttd --rule-test --sql` runs it: the statement
    /// only, no actions. It may call what `functions` has; its state is in a store of
    /// the replay's own, whichever `functions` carried.
    pub fn statement(sql: &str, functions: &Functions) -> Result<Self, String> {
        let (functions, store, now_ms) = on_scratch(functions);
        Ok(Self {
            rules: Rules::Statement(Box::new(SqlTest::compile(sql, &functions)?)),
            store,
            now_ms,
            warnings: Vec::new(),
        })
    }

    /// A replay of a rules file's text: every enabled rule, with its actions, and the
    /// namespaces the file declares, in a store of the replay's own.
    pub fn rules(text: &str, functions: &Functions) -> Result<Self, String> {
        let (functions, store, now_ms) = on_scratch(functions);
        let loaded = RuleSet::parse_with(text, &functions).map_err(|e| e.to_string())?;
        Ok(Self {
            rules: Rules::File(Box::new(loaded.rules)),
            store,
            now_ms,
            warnings: loaded.warnings,
        })
    }

    /// [`rules`](Self::rules) for a rules file on disk, whose modules are read from
    /// beside it.
    pub fn rules_file(path: &std::path::Path, functions: &Functions) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::rules(&text, &functions.beside(path))
    }

    /// What loading the rules file warned about.
    #[must_use]
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// Replay `steps`, in order. The replay is used up: its store held one sequence.
    #[must_use]
    pub fn run(self, steps: &[Step]) -> Report {
        let mut notes = Vec::new();
        if let Rules::File(set) = &self.rules {
            for (name, settings) in set.state_namespaces() {
                if settings.consistency != Consistency::Local {
                    notes.push(format!(
                        "namespace `{name}` is replicated in a cluster; a replay is one node, \
                         where it behaves as consistency = \"local\""
                    ));
                }
            }
        }
        let mut model = Model::new();
        let mut dictionaries: HashMap<&str, Dictionary> = HashMap::new();
        let mut run = Run {
            rules: &self.rules,
            now_ms: 0,
            next_id: 0,
            results: Vec::new(),
            failed: false,
            selected: false,
        };
        let mut reports = Vec::with_capacity(steps.len());
        for (index, step) in steps.iter().enumerate() {
            let now_ms = START_MS.saturating_add(step.at_ms);
            self.now_ms.store(now_ms, Relaxed);
            // The T4 hook: see `expire`.
            let mut state: Vec<String> = expire(&mut model, now_ms)
                .into_iter()
                .map(|(namespace, key, at_ms)| {
                    format!(
                        "state {namespace}: - {} (expired {})",
                        key_text(&namespace, &key),
                        clock(at_ms.saturating_sub(START_MS))
                    )
                })
                .collect();
            run.now_ms = i64::try_from(now_ms).unwrap_or(i64::MAX);
            let sent = match &step.send {
                None => "(nothing sent)".to_string(),
                Some(trigger) => {
                    let dictionary = dictionaries.entry(&trigger.clientid).or_insert_with(|| {
                        match &self.store {
                            Some(store) => Dictionary::counted_in(store.clone()),
                            None => Dictionary::new(),
                        }
                    });
                    run.send(trigger, dictionary)
                }
            };
            if let Some(store) = &self.store {
                changes(&mut model, store, &mut state);
            }
            reports.push(StepReport {
                number: index + 1,
                line: step.line,
                at_ms: step.at_ms,
                sent,
                results: std::mem::take(&mut run.results),
                state,
                failed: std::mem::take(&mut run.failed),
                selected: std::mem::take(&mut run.selected),
            });
        }
        Report {
            steps: reports,
            store: model
                .iter()
                .map(|((namespace, key), shown)| {
                    format!("{namespace}: {} = {shown}", key_text(namespace, key))
                })
                .collect(),
            end_ms: steps.last().map_or(0, |s| s.at_ms),
            notes,
        }
    }
}

/// Compare what `store` holds now with `model`, which becomes it, adding a line to
/// `state` for each key that is new, changed or gone.
fn changes(model: &mut Model, store: &Store, state: &mut Vec<String>) {
    let mut now = Model::new();
    for held in store.held() {
        let shown = Shown {
            value: held
                .value
                .to_json()
                .map_or_else(|e| format!("({e})"), |json| printable(json.as_bytes())),
            kind: held.kind,
            expires_ms: held.expires_ms,
        };
        now.insert((held.namespace, held.key), shown);
    }
    // One pass in the order of both: by namespace, then by key.
    let mut lines: BTreeMap<&(Arc<str>, Arc<[u8]>), String> = BTreeMap::new();
    for (at, shown) in &now {
        let sign = match model.get(at) {
            None => '+',
            Some(before) if before != shown => '~',
            Some(_) => continue,
        };
        lines.insert(
            at,
            format!(
                "state {}: {sign} {} = {shown}",
                at.0,
                key_text(&at.0, &at.1)
            ),
        );
    }
    for at in model.keys().filter(|at| !now.contains_key(*at)) {
        lines.insert(at, format!("state {}: - {}", at.0, key_text(&at.0, &at.1)));
    }
    state.extend(lines.into_values());
    *model = now;
}

/// One of a rule's actions, in a step's report.
enum Row {
    /// The n-th effect the trigger's rules rendered.
    Effect(usize),
    Failed(String),
    Skipped(Recursion),
}

impl Row {
    /// The action as the report prints it; `effects` are what the trigger's rules
    /// rendered, in order.
    fn line(&self, effects: &[(Arc<str>, Effect)]) -> Option<String> {
        Some(match self {
            Row::Effect(n) => match &effects.get(*n)?.1 {
                Effect::Republish(r) => format!(
                    "republish {} qos {}{} {}",
                    printable(r.topic.as_bytes()),
                    r.qos,
                    if r.retain { " retain" } else { "" },
                    printable(&r.payload)
                ),
                Effect::Console(line) => format!("console {}", printable(line.as_bytes())),
            },
            Row::Failed(e) => format!("action FAILED: {e}"),
            Row::Skipped(Recursion::SameRule) => {
                "republish skipped: the message is one this rule republished".to_string()
            }
            Row::Skipped(Recursion::Depth) => format!(
                "republish skipped: the message is {} republishes from the one that started \
                 the chain",
                mqtt_rules::MAX_REPUBLISH_DEPTH
            ),
        })
    }
}

/// What is evaluated: a message, or an event.
#[derive(Clone, Copy)]
enum Fired<'a> {
    Publish(&'a PublishInput<'a>),
    Event(&'a EventInput),
}

/// The replay of one sequence, as its steps run.
struct Run<'a> {
    rules: &'a Rules,
    /// The step's time on the sequence's clock.
    now_ms: i64,
    /// The last message id given out: a replay's ids count from 1, so that `id` is
    /// the same in every run.
    next_id: u128,
    results: Vec<String>,
    failed: bool,
    selected: bool,
}

impl Run<'_> {
    fn id(&mut self) -> u128 {
        self.next_id += 1;
        self.next_id
    }

    /// Evaluate what `trigger` sends on `dictionary`, its client's; how the report's
    /// heading shows what was sent.
    fn send(&mut self, trigger: &Trigger, dictionary: &Dictionary) -> String {
        // A statement that selects only events runs against the first one it names,
        // as `mqttd --rule-test` has it.
        let event = trigger.event.or_else(|| match self.rules {
            Rules::Statement(test) if !test.selects_topics() => test.events().first().copied(),
            _ => None,
        });
        let props = mqtt_core::AppProperties::default();
        let mut message = PublishInput::new(
            &trigger.clientid,
            &trigger.topic,
            &trigger.payload,
            trigger.qos,
            &props,
        );
        message.username = trigger.username.as_deref();
        message.retain = trigger.retain;
        message.node = NODE;
        message.received_at_ms = Some(self.now_ms);
        message.clock_ms = Some(self.now_ms);
        let sample;
        let (fired, heading) = match event {
            None => {
                message.message_id = Some(self.id());
                (
                    Fired::Publish(&message),
                    format!(
                        "{} {}",
                        printable(trigger.topic.as_bytes()),
                        printable(&trigger.payload)
                    ),
                )
            }
            Some(kind) => {
                // A client of the default plaintext listener, as `--rule-test` shows it.
                let client = ClientInfo {
                    clientid: &trigger.clientid,
                    username: trigger.username.as_deref(),
                    peer: "127.0.0.1:52345".parse().ok(),
                    sockname: "127.0.0.1:1883".parse().ok(),
                    node: NODE,
                };
                sample = EventInput::sample_message_at(
                    kind,
                    &client,
                    &trigger.topic,
                    trigger.qos,
                    &trigger.payload,
                    self.now_ms,
                    self.id(),
                );
                (
                    Fired::Event(&sample),
                    format!(
                        "event {} ({})",
                        kind.event_name(),
                        printable(trigger.clientid.as_bytes())
                    ),
                )
            }
        };
        match self.rules {
            Rules::Statement(test) => self.statement(test, fired, dictionary),
            Rules::File(file) => {
                let payload = match fired {
                    Fired::Publish(input) => input.payload.len(),
                    Fired::Event(input) => input.payload().map_or(0, Bytes::len),
                };
                let budget = Budget::new(payload);
                if !self.rule_set(file, fired, &budget, dictionary, 0) {
                    self.results.push("(no rule selects it)".to_string());
                }
            }
        }
        heading
    }

    /// One statement on one trigger: its outputs, one a line, as `--rule-test`
    /// prints them.
    fn statement(&mut self, test: &SqlTest, fired: Fired<'_>, dictionary: &Dictionary) {
        let input: &dyn Input = match fired {
            Fired::Publish(input) => input,
            Fired::Event(input) => input,
        };
        if let Some(why) = test.from_mismatch(input) {
            self.results.push(format!("(not selected: {why})"));
            return;
        }
        self.selected = true;
        match test.run(input, dictionary) {
            Ok(outputs) if outputs.is_empty() => self.results.push("(no output)".to_string()),
            Ok(outputs) => self
                .results
                .extend(outputs.iter().map(|o| printable(o.as_bytes()))),
            Err(e) => {
                self.failed = true;
                self.results.push(format!("FAILED: {e}"));
            }
        }
    }

    /// Every enabled rule of `set` that selects `fired`, and then, depth first, each
    /// message they republished that re-enters the rules: the order the broker
    /// evaluates in on a connection's task. Whether any rule ran on `fired` itself.
    fn rule_set(
        &mut self,
        set: &RuleSet,
        fired: Fired<'_>,
        budget: &Budget,
        dictionary: &Dictionary,
        depth: u32,
    ) -> bool {
        // (the rule, how its SQL ended, its actions in order)
        let mut ran: Vec<(Arc<str>, String, Vec<Row>)> = Vec::new();
        let mut rendered = 0usize;
        let mut failed = false;
        let mut effects = Vec::new();
        let mut report = |rule: &mqtt_rules::Rule, outcome: Outcome<'_>| {
            let mut sql = |result: String| ran.push((rule.id().clone(), result, Vec::new()));
            match outcome {
                Outcome::Passed => sql("passed".to_string()),
                Outcome::NoResult => sql("no_result".to_string()),
                Outcome::Failed(e) => {
                    failed = true;
                    sql(format!("FAILED: {e}"));
                }
                // An action's outcome follows its rule's `Passed`.
                Outcome::ActionOk => {
                    if let Some((_, _, rows)) = ran.last_mut() {
                        rows.push(Row::Effect(rendered));
                    }
                    rendered += 1;
                }
                Outcome::ActionFailed(e) => {
                    failed = true;
                    if let Some((_, _, rows)) = ran.last_mut() {
                        rows.push(Row::Failed(e.to_string()));
                    }
                }
                Outcome::Recursive(why) => {
                    if let Some((_, _, rows)) = ran.last_mut() {
                        rows.push(Row::Skipped(why));
                    }
                }
                // A replay is not timed, and its sandboxed calls are not traffic.
                Outcome::Elapsed(_) | Outcome::Function(_) => {}
            }
        };
        match fired {
            Fired::Publish(input) => {
                set.on_publish_in(input, budget, dictionary, &mut report, &mut effects);
            }
            Fired::Event(input) => {
                set.on_event_in(input, budget, dictionary, &mut report, &mut effects);
            }
        }
        self.failed |= failed;
        self.selected |= !ran.is_empty();
        let indent = "  ".repeat(usize::try_from(depth).unwrap_or(0));
        for (rule, result, rows) in &ran {
            self.results.push(format!("{indent}{rule}: {result}"));
            for line in rows.iter().filter_map(|row| row.line(&effects)) {
                self.results.push(format!("{indent}  {line}"));
            }
        }
        // As `rules::Engine::reenter` has it: each republished message, in order,
        // re-enters the rules unless its action dispatched it directly.
        let from = match fired {
            Fired::Publish(input) => input.republish_depth,
            Fired::Event(input) => input.republish_depth(),
        };
        for (rule, effect) in &effects {
            let Effect::Republish(r) = effect else {
                continue;
            };
            if r.direct_dispatch || !set.has_message_rules() {
                continue;
            }
            let mut input = PublishInput::republished(rule, r, from + 1);
            input.node = NODE;
            input.message_id = Some(self.id());
            input.received_at_ms = Some(self.now_ms);
            input.clock_ms = Some(self.now_ms);
            let at = self.results.len();
            if self.rule_set(set, Fired::Publish(&input), budget, dictionary, depth + 1) {
                self.results.insert(
                    at,
                    format!(
                        "{indent}> {} (republished by {rule})",
                        printable(r.topic.as_bytes())
                    ),
                );
            }
        }
        !ran.is_empty()
    }
}
