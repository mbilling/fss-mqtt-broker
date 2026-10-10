//! The rules endpoints ([ADR 0084](../../../../docs/adr/0084-watching-and-editing-rules-live.md)
//! D6): what the running rules are doing, the rules file, a check and a dry run of rules
//! text, and — for a listed writer — replacing the file or one rule in it.
//!
//! - **The file stays the only source.** A write replaces the configured rules file
//!   atomically ([`crate::atomic_file`]) and runs the ordinary reload, as `SIGHUP` after an
//!   edit by hand would; nothing is kept that the file does not say. Every write is based
//!   on the file **on disk**, read under the write lock: `if_match` is compared with its
//!   digest, a per-rule edit is made in its text ([`mqtt_rules::edit`]), and the answer
//!   says both what was written and what is running once the reload is done.
//! - **Who.** What the running rules are doing is a viewer's to read; their SQL, actions,
//!   warnings and error texts are an operator's, because a rules file can hold secrets (a
//!   pseudonym salt). The file itself, a check and a dry run are an operator's. A write
//!   needs the operator role AND a subject listed in `[rules] admin_writers`, which is
//!   empty — no writes — by default.
//! - **Bounded.** Parsing, splicing, checking, testing and writing run off the async
//!   workers and one at a time on the node, so at most one candidate rule set is in memory
//!   beside the running one.
//! - **A dry run changes nothing**: no metric, no last error, no trace record, no
//!   failure-report slot, no console log line.
//! - **Node-local.** Every answer names its node, and a write changes this node's file.

use super::http::Request;
use super::routes::{error, Answer};
use super::{AdminState, Caller, Role};
use crate::atomic_file::{self, Replace};
use crate::reload::{enabled_rules, sha256_hex, LastReload, ReloadOutcome};
use crate::rules::{rule_def, TraceOutput, TracePayload};
use crate::rules_sys::{counts_json, output_json, rfc3339_millis, ShownOutput};
use bytes::Bytes;
use mqtt_rules::edit::{EditError, RuleEdit};
use mqtt_rules::{Effect, EventKind, Input, LoadError, Loaded, Outcome, Rule, RuleSet};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// The reload trigger a rules write runs, as the audit log and the metrics name it.
pub const TRIGGER: &str = "admin-rules";

/// The bytes of a rendered payload or console line a dry run shows.
pub const TEST_OUTPUT_MAX: usize = 64 * 1024;

/// What the rules endpoints read beside the reloader.
pub struct RulesAccess {
    /// The rule engine: the running set, the metrics it counts into, what watches it.
    rules: crate::rules::Rules,
    /// The last reload attempt, whatever triggered it.
    last_reload: Arc<LastReload>,
    /// One parse, check, dry run or write at a time on this node. For a write it is the
    /// write lock too, held from reading the file on disk to reading back what runs.
    work: Arc<Semaphore>,
}

impl std::fmt::Debug for RulesAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RulesAccess").finish_non_exhaustive()
    }
}

impl RulesAccess {
    /// Serve `rules`, reporting `last_reload`.
    #[must_use]
    pub fn new(rules: crate::rules::Rules, last_reload: Arc<LastReload>) -> Self {
        Self {
            rules,
            last_reload,
            work: Arc::new(Semaphore::new(1)),
        }
    }
}

/// A refusal that names the node, with `extra`'s fields beside the error.
fn refuse(node: &str, status: u16, code: &str, message: &str, extra: Value) -> Answer {
    let mut body = json!({ "error": { "code": code, "message": message }, "node": node });
    if let (Some(body), Value::Object(extra)) = (body.as_object_mut(), extra) {
        body.extend(extra);
    }
    (status, body.to_string())
}

fn unwired() -> Answer {
    error(503, "unavailable", "the rules are not wired on this node")
}

/// Run `work` off the async workers, after the node's other rules work is done. The
/// permit travels with it, so a handler that times out does not let a second run start
/// while the first is still going.
async fn one_at_a_time(
    access: &RulesAccess,
    work: impl FnOnce() -> Answer + Send + 'static,
) -> Answer {
    let Ok(permit) = access.work.clone().acquire_owned().await else {
        return unwired();
    };
    tokio::task::spawn_blocking(move || {
        let answer = work();
        drop(permit);
        answer
    })
    .await
    .unwrap_or_else(|e| error(503, "unavailable", &format!("the rules task failed: {e}")))
}

/// The rules file the committed config names — never a reload's candidate, which may yet
/// be rejected. Blocks while a reload is in flight.
fn configured_file(state: &AdminState) -> Option<String> {
    match state
        .reload
        .as_ref()
        .and_then(|access| access.reloader.committed_config())
    {
        Some((config, _)) => config.rules.file,
        None => state
            .live_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rules
            .file
            .clone(),
    }
}

/// The file a write replaces: the configured path with every symlink resolved, so a
/// link's target is replaced and the link stays a link. A file that does not exist yet
/// is named in its directory's resolved path.
fn target_of(file: &str) -> std::io::Result<PathBuf> {
    let path = Path::new(file);
    match std::fs::canonicalize(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let name = path.file_name().ok_or(e)?;
            Ok(std::fs::canonicalize(atomic_file::dir_of(path))?.join(name))
        }
        resolved => resolved,
    }
}

/// The rules file's text on disk: `None` when there is no such file.
fn read_disk(path: &Path) -> std::io::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Why the broker could not write the rules file `file` now, when it could not: its
/// directory, where the replacement is written, refuses this process. `None` when it can.
#[must_use]
pub fn unwritable(file: &str) -> Option<String> {
    let dir = match target_of(file) {
        Ok(target) => atomic_file::dir_of(&target).to_path_buf(),
        Err(e) => return Some(format!("{file}: {e}")),
    };
    #[cfg(unix)]
    let refused = rustix::fs::access(
        &dir,
        rustix::fs::Access::WRITE_OK | rustix::fs::Access::EXEC_OK,
    )
    .err()
    .map(std::io::Error::from);
    #[cfg(not(unix))]
    let refused = match std::fs::metadata(&dir) {
        Ok(m) if m.permissions().readonly() => {
            Some(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
        }
        Ok(_) => None,
        Err(e) => Some(e),
    };
    refused.map(|e| format!("{}: {e}", dir.display()))
}

/// The line logged at boot and on every reload when rules writers are configured but the
/// rules file's directory is not writable by the broker: every write would be refused.
#[must_use]
pub fn unwritable_warning(rules: &mqtt_config::Rules) -> Option<String> {
    if rules.admin_writers.is_empty() {
        return None;
    }
    let why = unwritable(rules.file.as_deref()?)?;
    Some(format!(
        "rules.admin_writers is set but the rules file's directory is not writable by the \
         broker ({why}): every admin rules write will be refused with rules-file-unwritable \
         (ADR 0084)"
    ))
}

// ---------------------------------------------------------------------------------------
// GET /admin/v1/rules and GET /admin/v1/rules/source
// ---------------------------------------------------------------------------------------

/// Whether `caller` is listed in the live `[rules] admin_writers`.
fn is_writer(state: &AdminState, caller: &Caller) -> bool {
    let live = state
        .live_config
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    super::roles::listed(
        &live.rules.admin_writers,
        &caller.subject,
        caller.cn.as_deref(),
    )
}

/// `GET /admin/v1/rules`: the running rules — digests, each rule's definition hash, counts,
/// last activity and last error, the last reload. A viewer's answer is `redacted`: no SQL,
/// actions, warnings or error texts.
pub async fn list(state: &AdminState, caller: &Caller, role: Role) -> Answer {
    let Some(access) = state.rules.clone() else {
        return unwired();
    };
    let operator = role >= Role::Operator;
    let writer = operator && is_writer(state, caller);
    let state = state.clone();
    tokio::task::spawn_blocking(move || {
        let file = configured_file(&state);
        let file_digest = file
            .as_deref()
            .and_then(|f| read_disk(Path::new(f)).ok().flatten())
            .map(|text| sha256_hex(text.as_bytes()));
        let writable = writer && file.as_deref().is_some_and(|f| unwritable(f).is_none());
        let set = access.rules.current();
        let mut body = json!({
            "node": state.node_id,
            "digest": set.digest(),
            "file_digest": file_digest,
            "in_sync": file_digest.as_deref() == Some(set.digest()),
            "writable": writable,
            "rules": set.rules().iter().map(|r| rule_json(&access, r, operator)).collect::<Vec<_>>(),
            "reload": access.last_reload.get().map(|r| {
                let mut v = json!({
                    "at": rfc3339_millis(r.at),
                    "trigger": r.trigger,
                    "applied": r.applied,
                    "error_kind": r.error_kind(),
                    "repeats": r.repeats,
                });
                // The text can quote a config line holding a secret: an operator's.
                if operator {
                    v["error"] = json!(r.error);
                }
                v
            }),
        });
        if operator {
            body["warnings"] = json!(set.warnings());
        } else {
            body["redacted"] = json!(true);
        }
        (200, body.to_string())
    })
    .await
    .unwrap_or_else(|e| error(503, "unavailable", &format!("the rules task failed: {e}")))
}

/// One running rule, as [`list`] shows it.
fn rule_json(access: &RulesAccess, rule: &Rule, operator: bool) -> Value {
    let id = rule.id();
    let def = rule_def(rule);
    let observe = access.rules.observe();
    let counts = access
        .rules
        .metrics()
        .map(|m| m.rule_counts(id))
        .unwrap_or_default();
    // An error about an earlier definition of the rule is not this one's.
    let last_error = observe
        .and_then(|o| o.last_error(id))
        .filter(|e| e.def == def)
        .map(|e| {
            let mut v = json!({"at": rfc3339_millis(e.at), "kind": e.kind.as_str()});
            // It can quote a payload value.
            if operator {
                v["message"] = json!(e.message);
            }
            v
        });
    let mut v = json!({
        "id": &**id,
        "enabled": rule.enabled(),
        "description": rule.description(),
        "from": rule.topics(),
        "events": rule.events().iter().map(|k| k.event_name()).collect::<Vec<_>>(),
        "actions": rule.action_count(),
        "def": def,
        "counts": counts_json(&counts),
        "last_active_at": observe.and_then(|o| o.last_active(id)).map(rfc3339_millis),
        "last_error": last_error,
    });
    if operator {
        v["sql"] = json!(rule.sql());
        v["actions_spec"] = serde_json::to_value(rule.action_specs()).unwrap_or(Value::Null);
    } else {
        v["redacted"] = json!(true);
    }
    v
}

/// The rules file's text on disk, or the refusal that says why there is none.
fn disk_text(state: &AdminState) -> Result<(String, PathBuf, Option<String>), Answer> {
    let Some(file) = configured_file(state) else {
        return Err(refuse(
            &state.node_id,
            409,
            "rules-file-unset",
            "no rules file is configured (rules.file / MQTTD_RULES_FILE)",
            json!({}),
        ));
    };
    let target = target_of(&file).unwrap_or_else(|_| PathBuf::from(&file));
    match read_disk(&target) {
        Ok(text) => Ok((file, target, text)),
        Err(e) => Err(refuse(
            &state.node_id,
            409,
            "rules-file-unreadable",
            &format!("the rules file {file} cannot be read: {e}"),
            json!({}),
        )),
    }
}

fn missing(node: &str, file: &str) -> Answer {
    refuse(
        node,
        409,
        "rules-file-unreadable",
        &format!("the rules file {file} does not exist"),
        json!({}),
    )
}

/// `GET /admin/v1/rules/source`: the rules file as it is on disk, verbatim, with its
/// digest and the running one.
pub async fn source(state: &AdminState) -> Answer {
    let Some(access) = state.rules.clone() else {
        return unwired();
    };
    let state = state.clone();
    tokio::task::spawn_blocking(move || {
        let (file, _, text) = match disk_text(&state) {
            Ok(disk) => disk,
            Err(refusal) => return refusal,
        };
        let Some(text) = text else {
            return missing(&state.node_id, &file);
        };
        let digest = sha256_hex(text.as_bytes());
        let running = access.rules.current();
        (
            200,
            json!({
                "node": state.node_id,
                "file": file,
                "digest": digest,
                "running_digest": running.digest(),
                "in_sync": digest == running.digest(),
                "bytes": text.len(),
                "source": text,
            })
            .to_string(),
        )
    })
    .await
    .unwrap_or_else(|e| error(503, "unavailable", &format!("the rules task failed: {e}")))
}

// ---------------------------------------------------------------------------------------
// Rules text: a whole file, or one rule spliced into the file on disk
// ---------------------------------------------------------------------------------------

/// One rule as check and test take it: `PUT /admin/v1/rule`'s body plus its `id`, with
/// `description` and `enable` optional — absent, they are the on-disk rule's, or `""`
/// and `true` for a new one.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleBody {
    id: String,
    sql: String,
    actions: Vec<toml::Value>,
    description: Option<String>,
    enable: Option<bool>,
}

impl RuleBody {
    /// The full edit, the fields left out taken from `on_disk`'s table for this rule.
    fn edit(self, on_disk: &str) -> (String, RuleEdit) {
        let table: Option<toml::Table> = toml::from_str(on_disk).ok();
        let was = table
            .as_ref()
            .and_then(|t| t.get("rules")?.get(&self.id)?.as_table().cloned());
        let description = self.description.unwrap_or_else(|| {
            was.as_ref()
                .and_then(|t| t.get("description")?.as_str().map(String::from))
                .unwrap_or_default()
        });
        let enable = self.enable.unwrap_or_else(|| {
            was.as_ref()
                .and_then(|t| t.get("enable")?.as_bool())
                .unwrap_or(true)
        });
        (
            self.id,
            RuleEdit {
                sql: self.sql,
                actions: self.actions,
                description,
                enable,
            },
        )
    }
}

fn bad_request(node: &str, message: &str) -> Answer {
    refuse(node, 400, "bad-request", message, json!({}))
}

/// `?id=`, or the refusal: required, and a rule id.
fn rule_id<'a>(node: &str, req: &'a Request) -> Result<&'a str, Answer> {
    let Some(id) = req.param("id") else {
        return Err(bad_request(node, "id is required"));
    };
    if !mqtt_rules::valid_rule_id(id) {
        return Err(bad_request(
            node,
            &EditError::InvalidId(id.to_string()).to_string(),
        ));
    }
    Ok(id)
}

/// Why a per-rule edit was not made, as an answer.
fn edit_refused(node: &str, e: &EditError) -> Answer {
    let (status, code) = match e {
        EditError::InvalidId(_) | EditError::Unrepresentable(_) => (400, "bad-request"),
        EditError::FileInvalid(_) => (409, "rules-file-invalid"),
        EditError::LayoutUnsupported(_) => (409, "rules-layout-unsupported"),
        EditError::NoSuchRule(_) => (404, "not-found"),
        EditError::Failed { .. } => (500, "rules-edit-failed"),
    };
    refuse(node, status, code, &e.to_string(), json!({}))
}

/// `422 rules-invalid`: what is wrong with `text`, and where — a TOML error at its file
/// line and column, a SQL error at its line and column within the rule's statement.
fn invalid(node: &str, text: &str, e: &LoadError) -> Answer {
    let details = match e {
        LoadError::File { .. } => {
            let mut d = json!({"scope": "toml"});
            if let Some((line, column)) = e.file_position(text) {
                d["line"] = json!(line);
                d["column"] = json!(column);
            }
            d
        }
        LoadError::Rule {
            id,
            sql_line: Some(line),
            sql_column,
            ..
        } => json!({"scope": "sql", "rule": id, "line": line, "column": sql_column}),
        // About the rule but not its SQL: its id, its actions, how many it has.
        LoadError::Rule { id, .. } => json!({"scope": "rule", "rule": id}),
    };
    (
        422,
        json!({
            "error": {"code": "rules-invalid", "message": e.to_string(), "details": details},
            "node": node,
        })
        .to_string(),
    )
}

/// Parse `text` as the broker would load it, or answer `422 rules-invalid`.
fn parse(node: &str, text: &str) -> Result<Loaded, Answer> {
    RuleSet::parse(text).map_err(|e| invalid(node, text, &e))
}

/// The text a check or a dry run is about: `source`, or `rule` spliced into the file on
/// disk; with the spliced rule's id.
fn candidate(
    state: &AdminState,
    source: Option<String>,
    rule: Option<RuleBody>,
) -> Result<(String, Option<String>), Answer> {
    match (source, rule) {
        (Some(source), _) => Ok((source, None)),
        (None, Some(rule)) => {
            let (file, _, text) = disk_text(state)?;
            let Some(text) = text else {
                return Err(missing(&state.node_id, &file));
            };
            let (id, edit) = rule.edit(&text);
            let spliced = mqtt_rules::edit::put_rule(&text, &id, &edit)
                .map_err(|e| edit_refused(&state.node_id, &e))?;
            Ok((spliced, Some(id)))
        }
        (None, None) => Err(bad_request(&state.node_id, "source or rule is required")),
    }
}

/// `POST /admin/v1/rules/check` with `{"source"}` or `{"rule"}`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckBody {
    source: Option<String>,
    rule: Option<RuleBody>,
}

/// Decode a JSON body, or answer `400 bad-request`.
fn body<T: serde::de::DeserializeOwned>(node: &str, req: &Request) -> Result<T, Answer> {
    serde_json::from_slice(&req.body)
        .map_err(|e| bad_request(node, &format!("the body is not the JSON expected: {e}")))
}

/// `POST /admin/v1/rules/check`: would this text load? `{"source"}` checks a whole file;
/// `{"rule"}` checks the file on disk with that rule put in, as a write would make it.
/// Nothing is written. `200 {valid:true, …}`, or `422 rules-invalid` with the error a
/// write would answer.
pub async fn check(state: &AdminState, req: &Request) -> Answer {
    let Some(access) = state.rules.clone() else {
        return unwired();
    };
    let checked: CheckBody = match body(&state.node_id, req) {
        Ok(b) => b,
        Err(refusal) => return refusal,
    };
    if checked.source.is_some() && checked.rule.is_some() {
        return bad_request(&state.node_id, "give source or rule, not both");
    }
    let state = state.clone();
    one_at_a_time(&access, move || {
        let (text, _) = match candidate(&state, checked.source, checked.rule) {
            Ok(c) => c,
            Err(refusal) => return refusal,
        };
        let loaded = match parse(&state.node_id, &text) {
            Ok(l) => l,
            Err(refusal) => return refusal,
        };
        (
            200,
            json!({
                "node": state.node_id,
                "valid": true,
                "rules": loaded.rules.len(),
                "enabled": enabled_rules(&loaded.rules),
                "digest": loaded.rules.digest(),
                "warnings": loaded.warnings,
            })
            .to_string(),
        )
    })
    .await
}

// ---------------------------------------------------------------------------------------
// POST /admin/v1/rules/test: the dry run
// ---------------------------------------------------------------------------------------

/// `POST /admin/v1/rules/test`'s body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestBody {
    source: Option<String>,
    rule: Option<RuleBody>,
    only: Option<String>,
    topic: String,
    payload: String,
    payload_encoding: Option<String>,
    qos: Option<u8>,
    retain: Option<bool>,
    clientid: Option<String>,
    username: Option<String>,
    event: Option<String>,
}

/// What a dry run evaluates on, checked.
struct Trigger {
    topic: String,
    payload: Bytes,
    qos: u8,
    retain: bool,
    clientid: String,
    username: Option<String>,
    event: Option<EventKind>,
}

impl Trigger {
    fn of(node: &str, t: &TestBody) -> Result<Self, Answer> {
        let payload = match t.payload_encoding.as_deref() {
            None | Some("utf8") => Bytes::from(t.payload.clone()),
            Some("base64") => crate::backup::b64_decode(&t.payload)
                .map(Bytes::from)
                .map_err(|e| bad_request(node, &format!("payload: {e}")))?,
            Some(other) => {
                return Err(bad_request(
                    node,
                    &format!("payload_encoding is utf8 or base64, not {other:?}"),
                ))
            }
        };
        let qos = t.qos.unwrap_or(0);
        if qos > 2 {
            return Err(bad_request(node, "qos is 0, 1 or 2"));
        }
        let event = match t.event.as_deref() {
            None => None,
            Some(name) => Some(EventKind::parse(name).ok_or_else(|| {
                let names: Vec<&str> = EventKind::ALL.iter().map(|k| k.event_name()).collect();
                bad_request(
                    node,
                    &format!("event is one of {}, not {name:?}", names.join(", ")),
                )
            })?),
        };
        if event.is_none() {
            if !mqtt_core::valid_topic_name(&t.topic) {
                return Err(bad_request(
                    node,
                    "topic is not a topic name a client could publish to",
                ));
            }
            // No client can publish there, so no rule ever sees such a message.
            if mqtt_core::is_reserved_topic(&t.topic) {
                return Err(refuse(
                    node,
                    400,
                    "topic-reserved",
                    "$SYS/ is reserved for the broker (ADR 0084): no client publish there \
                     runs a rule",
                    json!({}),
                ));
            }
        }
        Ok(Self {
            topic: t.topic.clone(),
            payload,
            qos,
            retain: t.retain.unwrap_or(false),
            clientid: t
                .clientid
                .clone()
                .unwrap_or_else(|| "test-client".to_string()),
            username: t.username.clone(),
            event,
        })
    }
}

/// `POST /admin/v1/rules/test`: what the rules would do with one message or event,
/// changing nothing. The rules are the running set, `source`, or the file on disk with
/// `rule` put in; `only` picks one rule of a set, and `rule` is tested alone. A rule
/// tested alone runs whether or not it is enabled (its `enabled` is echoed); a set runs
/// its enabled rules whose `FROM` selects the trigger.
pub async fn test(state: &AdminState, req: &Request) -> Answer {
    let Some(access) = state.rules.clone() else {
        return unwired();
    };
    let node = state.node_id.clone();
    let t: TestBody = match body(&node, req) {
        Ok(b) => b,
        Err(refusal) => return refusal,
    };
    if t.source.is_some() && t.rule.is_some() {
        return bad_request(&node, "give source or rule, not both");
    }
    if t.rule.is_some() && t.only.is_some() {
        return bad_request(&node, "only picks a rule of a set; rule is one already");
    }
    if let Some(only) = t
        .only
        .as_deref()
        .filter(|id| !mqtt_rules::valid_rule_id(id))
    {
        return bad_request(&node, &EditError::InvalidId(only.to_string()).to_string());
    }
    let trigger = match Trigger::of(&node, &t) {
        Ok(trigger) => trigger,
        Err(refusal) => return refusal,
    };
    let state = state.clone();
    let running = access.rules.current();
    one_at_a_time(&access, move || {
        let (set, alone) = if t.source.is_none() && t.rule.is_none() {
            (running, t.only)
        } else {
            let (text, spliced) = match candidate(&state, t.source, t.rule) {
                Ok(c) => c,
                Err(refusal) => return refusal,
            };
            match parse(&node, &text) {
                Ok(loaded) => (Arc::new(loaded.rules), spliced.or(t.only)),
                Err(refusal) => return refusal,
            }
        };
        if let Some(id) = alone.as_deref().filter(|id| set.get(id).is_none()) {
            return refuse(
                &node,
                404,
                "not-found",
                &format!("the rules have no rule `{id}`"),
                json!({}),
            );
        }
        let results = dry_run(&set, alone.as_deref(), &trigger, &node);
        let answer = TestAnswer {
            node: &node,
            results,
        };
        (200, serde_json::to_string(&answer).unwrap_or_default())
    })
    .await
}

/// Evaluate `trigger` against `alone`, or every enabled rule of `set` whose `FROM`
/// selects it, with a report of its own: nothing outside this function sees it.
fn dry_run(set: &RuleSet, alone: Option<&str>, trigger: &Trigger, node: &str) -> Vec<Tested> {
    let props = mqtt_core::AppProperties::default();
    let mut message = mqtt_rules::PublishInput::new(
        &trigger.clientid,
        &trigger.topic,
        &trigger.payload,
        trigger.qos,
        &props,
    );
    message.username = trigger.username.as_deref();
    message.retain = trigger.retain;
    message.node = node;
    let sample;
    let input: &dyn Input = match trigger.event {
        Some(kind) => {
            let client = mqtt_rules::ClientInfo {
                clientid: &trigger.clientid,
                username: trigger.username.as_deref(),
                peer: None,
                sockname: None,
                node,
            };
            sample = mqtt_rules::EventInput::sample_message(
                kind,
                &client,
                &trigger.topic,
                trigger.qos,
                &trigger.payload,
            );
            &sample
        }
        None => &message,
    };
    let ids: Vec<&str> = match alone {
        Some(id) => vec![id],
        None => set
            .rules()
            .iter()
            .filter(|r| r.enabled() && r.from_mismatch(input).is_none())
            .map(|r| &**r.id())
            .collect(),
    };
    ids.into_iter()
        .filter_map(|id| set.get(id))
        .map(|rule| evaluate(set, rule, input))
        .collect()
}

/// One rule's slot in a dry run's outputs, in the order its actions reported.
enum Slot {
    /// The n-th effect it rendered.
    Effect(usize),
    /// Which action failed, and why.
    Failed(usize, String),
}

/// A dry run's answer. Structs rather than `json!`, so a console output stays the text
/// the rule rendered ([`ShownOutput`]).
#[derive(Serialize)]
struct TestAnswer<'a> {
    node: &'a str,
    results: Vec<Tested>,
}

/// What one rule did in a dry run.
#[derive(Serialize)]
struct Tested {
    enabled: bool,
    error: Option<String>,
    outputs: Vec<ShownOutput>,
    /// Why the rule would never run on this trigger, with `result` `no_match`.
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    result: &'static str,
    rule: String,
}

fn evaluate(set: &RuleSet, rule: &Rule, input: &dyn Input) -> Tested {
    let id = &**rule.id();
    if let Some(reason) = rule.from_mismatch(input) {
        return Tested {
            enabled: rule.enabled(),
            error: None,
            outputs: Vec::new(),
            reason: Some(reason),
            result: "no_match",
            rule: id.to_string(),
        };
    }
    let (mut result, mut error, mut slots) = ("no_result", None, Vec::new());
    // Each output runs every action in order; an `ActionOk` is followed by its effect.
    let (mut actions, mut rendered, per_output) = (0usize, 0usize, rule.action_count().max(1));
    let mut effects = Vec::new();
    set.evaluate_one(
        id,
        input,
        &mut |_, outcome| match outcome {
            Outcome::Passed => result = "passed",
            Outcome::NoResult => result = "no_result",
            Outcome::Failed(e) => {
                result = "failed";
                error = Some(e.to_string());
            }
            Outcome::ActionOk => {
                slots.push(Slot::Effect(rendered));
                rendered += 1;
                actions += 1;
            }
            Outcome::ActionFailed(e) => {
                slots.push(Slot::Failed(actions % per_output, e.to_string()));
                actions += 1;
            }
            // A dry run's input is never a republished message, so no republish is
            // skipped as a loop; were one, it would show nothing.
            Outcome::Recursive(_) => actions += 1,
            // A dry run is not timed (`evaluate_one` never reports it).
            Outcome::Elapsed(_) => {}
        },
        &mut effects,
    );
    let outputs: Vec<ShownOutput> = slots
        .into_iter()
        .filter_map(|slot| match slot {
            Slot::Effect(n) => effects.get(n).map(|(_, e)| output_json(&shown(e))),
            Slot::Failed(action_index, error) => Some(output_json(&TraceOutput::Failed {
                action_index,
                error,
            })),
        })
        .collect();
    Tested {
        enabled: rule.enabled(),
        error,
        outputs,
        reason: None,
        result,
        rule: id.to_string(),
    }
}

/// A rendered effect as a dry run shows it: the trace's shape, payloads and console
/// lines up to [`TEST_OUTPUT_MAX`] bytes.
fn shown(effect: &Effect) -> TraceOutput {
    match effect {
        Effect::Republish(r) => TraceOutput::Republish {
            topic: r.topic.clone(),
            qos: r.qos,
            retain: r.retain,
            payload: TracePayload {
                bytes: Bytes::copy_from_slice(&r.payload[..r.payload.len().min(TEST_OUTPUT_MAX)]),
                len: r.payload.len(),
            },
        },
        Effect::Console(line) => TraceOutput::Console {
            output: crate::rules::clip(line, TEST_OUTPUT_MAX).to_string(),
            len: line.len(),
        },
    }
}

// ---------------------------------------------------------------------------------------
// The writes: PUT /admin/v1/rules, PUT /admin/v1/rule, DELETE /admin/v1/rule
// ---------------------------------------------------------------------------------------

/// What a write does to the file on disk.
enum Op {
    /// Replace it with this text.
    PutFile(String),
    /// Insert or update one rule.
    PutRule(String, RuleEdit),
    /// Remove one rule.
    DeleteRule(String),
}

impl Op {
    fn name(&self) -> &'static str {
        match self {
            Op::PutFile(_) => "put-file",
            Op::PutRule(..) => "put-rule",
            Op::DeleteRule(_) => "delete-rule",
        }
    }

    fn rule(&self) -> &str {
        match self {
            Op::PutFile(_) => "-",
            Op::PutRule(id, _) | Op::DeleteRule(id) => id,
        }
    }
}

/// `PUT /admin/v1/rules`'s body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileBody {
    source: String,
}

/// `PUT /admin/v1/rules?if_match=<digest>|*` with `{"source"}`: replace the whole file.
/// `if_match` is required — the file's digest as last read, or `*` to replace whatever is
/// there.
pub async fn put_file(state: &AdminState, caller: &Caller, req: &Request) -> Answer {
    if req.param("if_match").is_none() {
        return writer_refusal(state, caller).unwrap_or_else(|| {
            refuse(
                &state.node_id,
                428,
                "precondition-required",
                "if_match is required: the digest of the file being replaced (GET \
                 /admin/v1/rules/source), or * to replace whatever is there",
                json!({}),
            )
        });
    }
    match body::<FileBody>(&state.node_id, req) {
        Ok(b) => write(state, caller, req, Op::PutFile(b.source)).await,
        Err(refusal) => writer_refusal(state, caller).unwrap_or(refusal),
    }
}

/// `PUT /admin/v1/rule?id=<id>[&if_match=<digest>]` with `{sql, actions, description,
/// enable}`: insert the rule, or update it in place.
pub async fn put_rule(state: &AdminState, caller: &Caller, req: &Request) -> Answer {
    let parsed = rule_id(&state.node_id, req).and_then(|id| {
        body::<RuleEdit>(&state.node_id, req).map(|edit| Op::PutRule(id.to_string(), edit))
    });
    match parsed {
        Ok(op) => write(state, caller, req, op).await,
        Err(refusal) => writer_refusal(state, caller).unwrap_or(refusal),
    }
}

/// `DELETE /admin/v1/rule?id=<id>[&if_match=<digest>]`: remove the rule.
pub async fn delete_rule(state: &AdminState, caller: &Caller, req: &Request) -> Answer {
    match rule_id(&state.node_id, req) {
        Ok(id) => write(state, caller, req, Op::DeleteRule(id.to_string())).await,
        Err(refusal) => writer_refusal(state, caller).unwrap_or(refusal),
    }
}

/// Why `caller` may not write the rules here, if they may not: writes are off (no
/// writers listed), or they are not one of the writers. Asked before anything about the
/// request itself is answered.
fn writer_refusal(state: &AdminState, caller: &Caller) -> Option<Answer> {
    let none_listed = state
        .live_config
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .rules
        .admin_writers
        .is_empty();
    if none_listed {
        return Some(refuse(
            &state.node_id,
            403,
            "rules-read-only",
            "rules writes are off on this node: [rules] admin_writers \
             (MQTTD_RULES_ADMIN_WRITERS) lists no one",
            json!({}),
        ));
    }
    (!is_writer(state, caller)).then(|| {
        refuse(
            &state.node_id,
            403,
            "not-a-rules-writer",
            "this certificate's subject is not listed in [rules] admin_writers",
            json!({}),
        )
    })
}

/// Make one write: check the writer, then — one at a time, off the async workers — read
/// the file on disk, check `if_match`, make the new text, check it loads, replace the
/// file, reload, and read back what runs.
async fn write(state: &AdminState, caller: &Caller, req: &Request, op: Op) -> Answer {
    if let Some(refusal) = writer_refusal(state, caller) {
        return refusal;
    }
    let Some(access) = state.rules.clone() else {
        return unwired();
    };
    let Some(reloader) = state.reload.as_ref().map(|r| r.reloader.clone()) else {
        return error(503, "unavailable", "reload is not wired on this node");
    };
    let if_match = req.param("if_match").map(String::from);
    let subject = caller.subject.clone();
    let state = state.clone();
    let rules = access.rules.clone();
    one_at_a_time(&access, move || {
        let write = RulesWrite {
            state: &state,
            rules: &rules,
            reloader: &reloader,
            subject: &subject,
        };
        write.locked(&op, if_match.as_deref())
    })
    .await
}

/// What one write works with.
struct RulesWrite<'a> {
    state: &'a AdminState,
    rules: &'a crate::rules::Rules,
    reloader: &'a crate::reload::Reloader,
    /// The writer's certificate subject, for the audit record.
    subject: &'a str,
}

impl RulesWrite<'_> {
    /// [`write`]'s blocking half, under the node's rules lock.
    fn locked(&self, op: &Op, if_match: Option<&str>) -> Answer {
        let (state, rules) = (self.state, self.rules);
        let node = &state.node_id;
        let (file, target, old) = match disk_text(state) {
            Ok(disk) => disk,
            Err(refusal) => return refusal,
        };
        let old_digest = old.as_deref().map(|t| sha256_hex(t.as_bytes()));
        if let Some(expected) = if_match.filter(|m| *m != "*") {
            if old_digest.as_deref() != Some(expected) {
                return refuse(
                    node,
                    412,
                    "digest-mismatch",
                    "the rules file on disk is not the one if_match names: read it again \
                     (GET /admin/v1/rules/source) and redo the change",
                    json!({
                        "file_digest": old_digest,
                        "running_digest": rules.current().digest(),
                    }),
                );
            }
        }
        let edited = match op {
            Op::PutFile(text) => Ok(text.clone()),
            Op::PutRule(id, edit) => match &old {
                Some(old) => mqtt_rules::edit::put_rule(old, id, edit),
                None => return missing(node, &file),
            },
            Op::DeleteRule(id) => match &old {
                Some(old) => mqtt_rules::edit::delete_rule(old, id),
                None => return missing(node, &file),
            },
        };
        let text = match edited {
            Ok(text) => text,
            Err(e) => return edit_refused(node, &e),
        };
        let loaded = match parse(node, &text) {
            Ok(l) => l,
            Err(refusal) => return refusal,
        };
        let digest = loaded.rules.digest().to_string();
        let written = old.as_deref() != Some(text.as_str());
        if written {
            let how = Replace {
                // A file written where there was none is the broker's alone: a rules file
                // can hold a secret. One that existed keeps its mode.
                new_mode: 0o600,
                previous: old.as_deref().map(str::as_bytes),
            };
            if let Err(e) = atomic_file::replace(&target, text.as_bytes(), &how) {
                return unwritable_answer(node, &file, &e);
            }
        }
        // Reload when the file changed — or did not, but is not what runs.
        let outcome: Option<ReloadOutcome> = (written || rules.current().digest() != digest)
            .then(|| self.reloader.reload_with_outcome(TRIGGER));
        let running_digest = rules.current().digest().to_string();
        let applied = outcome.as_ref().is_none_or(|o| o.applied) && running_digest == digest;
        if written {
            state.audit.record(
                "rules.write",
                Some(self.subject),
                &format!(
                    "op={} rule={} old={} new={digest} applied={applied}",
                    op.name(),
                    op.rule(),
                    old_digest.as_deref().unwrap_or("-"),
                ),
            );
        }
        match outcome {
            Some(outcome) if !outcome.applied => refuse(
                node,
                409,
                "reload-rejected",
                outcome.error.as_deref().unwrap_or_default(),
                json!({
                    "written": written,
                    "digest": digest,
                    "running_digest": running_digest,
                    "outcome": outcome,
                }),
            ),
            outcome => (
                200,
                json!({
                    "node": node,
                    "written": written,
                    "digest": digest,
                    "running_digest": running_digest,
                    "applied": applied,
                    "rules": loaded.rules.len(),
                    "enabled": enabled_rules(&loaded.rules),
                    "warnings": loaded.warnings,
                    "reload": outcome,
                })
                .to_string(),
            ),
        }
    }
}

/// Whether an I/O error says the file cannot be written here (permissions, a read-only
/// filesystem or mount, a busy file, a link across filesystems, a directory that is
/// gone) rather than that writing it failed.
fn is_unwritable(kind: std::io::ErrorKind) -> bool {
    use std::io::ErrorKind as K;
    matches!(
        kind,
        K::PermissionDenied
            | K::ReadOnlyFilesystem
            | K::ResourceBusy
            | K::CrossesDevices
            | K::NotFound
    )
}

/// The answer to a replace that failed: the rules file is as it was (a failure of the
/// last rename comes after `<file>.prev` was replaced).
fn unwritable_answer(node: &str, file: &str, e: &atomic_file::ReplaceError) -> Answer {
    let (status, code) = if is_unwritable(e.error.kind()) {
        (409, "rules-file-unwritable")
    } else {
        (500, "rules-write-failed")
    };
    refuse(
        node,
        status,
        code,
        &format!("the rules file {file} was not written, it is as it was: {e}"),
        json!({"os_error": e.error.to_string()}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The errors that say the file cannot be written here are a conflict to resolve;
    /// any other failure to write is the server's.
    #[test]
    fn a_write_the_filesystem_refuses_is_a_conflict_and_any_other_failure_the_servers() {
        use std::io::ErrorKind as K;
        let answer = |kind: K| {
            let e = atomic_file::ReplaceError {
                step: "create",
                path: PathBuf::from("/etc/mqttd/.rules.toml.1.x.tmp"),
                error: std::io::Error::from(kind),
            };
            let (status, body) = unwritable_answer("n1", "/etc/mqttd/rules.toml", &e);
            let body: Value = serde_json::from_str(&body).unwrap();
            (status, body["error"]["code"].as_str().unwrap().to_string())
        };
        for kind in [
            K::PermissionDenied,
            K::ReadOnlyFilesystem,
            K::ResourceBusy,
            K::CrossesDevices,
            K::NotFound,
        ] {
            assert_eq!(
                answer(kind),
                (409, "rules-file-unwritable".into()),
                "{kind:?}"
            );
        }
        for kind in [K::StorageFull, K::Other, K::Interrupted] {
            assert_eq!(answer(kind), (500, "rules-write-failed".into()), "{kind:?}");
        }
    }

    /// A rule body's left-out description and enable are the on-disk rule's, or the
    /// defaults for a new rule.
    #[test]
    fn a_rule_body_leaves_out_what_the_file_already_says() {
        let disk = "[rules.a]\ndescription = 'kept'\nenable = false\nsql = 'SELECT 1 FROM \"t\"'\n";
        let body = |id: &str| RuleBody {
            id: id.into(),
            sql: "SELECT 2 FROM \"t\"".into(),
            actions: Vec::new(),
            description: None,
            enable: None,
        };
        let (_, edit) = body("a").edit(disk);
        assert_eq!((edit.description.as_str(), edit.enable), ("kept", false));
        let (_, edit) = body("b").edit(disk);
        assert_eq!((edit.description.as_str(), edit.enable), ("", true));
    }

    /// No writers, or a directory the broker cannot write in: the boot and reload line.
    #[test]
    fn writers_with_an_unwritable_directory_are_warned_about() {
        let mut rules = mqtt_config::Rules {
            file: Some("/nonexistent-mqttd-dir/rules.toml".into()),
            ..mqtt_config::Rules::default()
        };
        assert_eq!(
            unwritable_warning(&rules),
            None,
            "no writers, nothing to warn"
        );
        rules.admin_writers = vec!["CN=ui".into()];
        let line = unwritable_warning(&rules).expect("a directory that is not there");
        assert!(line.contains("/nonexistent-mqttd-dir"), "{line}");
        let dir = tempfile::tempdir().unwrap();
        rules.file = Some(dir.path().join("rules.toml").display().to_string());
        assert_eq!(unwritable_warning(&rules), None, "a writable directory");
    }
}
