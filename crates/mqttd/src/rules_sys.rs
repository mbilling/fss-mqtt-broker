//! Watching the running rules on `$SYS` ([ADR 0084](../../../docs/adr/0084-watching-and-editing-rules-live.md)):
//! the per-rule statistics and the rule trace.
//!
//! Two tasks, spawned at boot and stopped by the shutdown token, which the broker cancels
//! right after telling the hub it is draining:
//!
//! - [`run_stats`] publishes, every `[rules] sys_interval_secs`, a summary on
//!   `$SYS/brokers/<node>/rules` and one message per rule — enabled or not — on
//!   `$SYS/brokers/<node>/rules/<id>`. The counts are the Prometheus counters read back
//!   without creating a series, cumulative since the broker started and keyed by rule
//!   id; the rates are their growth over the measured time between two ticks. An
//!   interval change applies at once.
//! - [`run_trace`] turns the [`TraceRecord`]s evaluations queue into JSON on
//!   `$SYS/brokers/<node>/trace/rules/<id>`, at most max(`trace_rate`, 200) a second for
//!   the node.
//!
//! Every message is `QoS` 0, never retained, routed by [`HubCommand::SysPublish`], and
//! takes node-pool ingress credit first (ADR 0082): when the pool is short the rest of a
//! statistics tick is skipped and a trace record dropped, each counted, so both yield to
//! clients under pressure. They are live only — a session that is not connected gets
//! none of them — and carry a Message Expiry Interval, so a copy queued anyway (by a
//! peer that predates live-only delivery) expires: two intervals for the statistics, at
//! least 10 seconds, and 10 seconds for a trace record.
//!
//! Nothing secret goes on `$SYS`: no SQL, description, actions, file path, writer or
//! reload error text. A rule's last error is its time and kind only: its text can quote
//! a payload value, and a statistics reader need not be one the trace's payloads are
//! for. The trace carries the text, and the admin API shows it to operators.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mqtt_observability::metrics::{Metrics, RuleCounts};
use mqtt_rules::RuleSet;
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::hub::HubCommand;
use crate::ingress::IngressCredit;
use crate::reload::LastReload;
use crate::rules::{
    rule_def, ErrorKind, LastError, Rules, RulesObserve, TraceOutput, TracePayload, TraceRecord,
    TraceTrigger, TracedMessage, FAILURE_WARN_INTERVAL_SECS,
};

/// The node-wide floor of the trace's ceiling, in records a second: the ceiling is
/// max(`trace_rate`, this), on top of each rule's own `trace_rate`.
pub const TRACE_NODE_FLOOR: u32 = 200;

/// The Message Expiry Interval of a trace record, in seconds, and the least one of a
/// statistics message.
const SYS_EXPIRY_SECS: u32 = 10;

/// The Message Expiry Interval of a statistics message published every `interval`
/// seconds: two intervals, so the next tick replaces it first, and at least
/// [`SYS_EXPIRY_SECS`].
fn stats_expiry(interval: u64) -> u32 {
    u32::try_from(interval.saturating_mul(2))
        .unwrap_or(u32::MAX)
        .max(SYS_EXPIRY_SECS)
}

/// RFC 3339 in UTC with milliseconds (`2026-10-08T12:34:56.789Z`), never the host's zone.
#[must_use]
pub fn rfc3339_millis(t: SystemTime) -> String {
    let ms = t
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let secs = ms / 1000;
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    let of_day = secs % 86_400;
    let (y, m, d) = crate::backup::civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        of_day / 3600,
        (of_day % 3600) / 60,
        of_day % 60,
        ms % 1000
    )
}

/// Send one `$SYS` message that expires after `message_expiry` seconds, charged to the
/// node pool; `false` when the pool is short.
fn sys_publish(
    hub: &mpsc::UnboundedSender<HubCommand>,
    ingress: &IngressCredit,
    topic: String,
    payload: String,
    message_expiry: u32,
) -> bool {
    let Some(permit) = ingress.try_acquire_pool(ingress.cost(topic.len(), payload.len())) else {
        return false;
    };
    let _ = hub.send(HubCommand::SysPublish {
        topic,
        payload: Bytes::from(payload),
        message_expiry,
        credit: Some(permit),
    });
    true
}

/// Sleep until `at`, or for ever when there is nothing to wait for.
async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Publish the rule statistics until `shutdown` (ADR 0084 D4). Returns at once when the
/// rules are not watched ([`Rules::with_observe`]).
pub async fn run_stats(
    rules: Rules,
    metrics: Option<Arc<Metrics>>,
    hub: mpsc::UnboundedSender<HubCommand>,
    ingress: Arc<IngressCredit>,
    last_reload: Arc<LastReload>,
    started_at: SystemTime,
    shutdown: CancellationToken,
) {
    let Some(observe) = rules.observe().cloned() else {
        return;
    };
    let mut settings = observe.subscribe();
    let mut stats = Stats {
        node: rules.node().to_string(),
        started_at: rfc3339_millis(started_at),
        ..Stats::default()
    };
    let mut last_tick: Option<Instant> = None;
    loop {
        let interval = settings.borrow_and_update().sys_interval_secs;
        if interval == 0 {
            // Off: what runs meanwhile is not seen, so the next tick takes a new baseline.
            stats.seeded = false;
        }
        // Measured from the last tick, so a shorter interval that is already due fires
        // now rather than after the old one.
        let next = (interval > 0)
            .then(|| last_tick.map_or_else(Instant::now, |t| t + Duration::from_secs(interval)));
        tokio::select! {
            () = shutdown.cancelled() => return,
            changed = settings.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = until(next) => {
                let now = Instant::now();
                let ctx = Tick {
                    rules: &rules,
                    metrics: metrics.as_deref(),
                    last_reload: &last_reload,
                    interval,
                };
                let expiry = stats_expiry(interval);
                for (topic, payload) in stats.tick(&ctx, now, SystemTime::now()) {
                    if !sys_publish(&hub, &ingress, topic, payload, expiry) {
                        // The rest of this tick is skipped, and the next is whole.
                        observe.count_stats_dropped();
                        break;
                    }
                }
                last_tick = Some(now);
            }
        }
    }
}

/// What one statistics tick reads.
struct Tick<'a> {
    rules: &'a Rules,
    metrics: Option<&'a Metrics>,
    last_reload: &'a LastReload,
    interval: u64,
}

/// The rule ids whose last activity the statistics remember: four times the rules a
/// file may hold. Ids a reload removed are kept, so one put back is not taken for one
/// that ran; past this, only the running set's are.
const SEEN_MAX: usize = 4 * mqtt_rules::MAX_RULES;

/// What the statistics remember from one tick to the next.
#[derive(Default)]
struct Stats {
    node: String,
    started_at: String,
    /// Each rule's counts at the previous tick, and when it was.
    prev: HashMap<Arc<str>, RuleCounts>,
    prev_at: Option<(Instant, SystemTime)>,
    /// Each rule id's evaluation count when last looked at — what its growth, and so
    /// its last activity, is measured against. Kept across reloads, at most
    /// [`SEEN_MAX`] ids.
    seen: HashMap<Arc<str>, u64>,
    /// Whether `seen` holds a baseline: not before the first tick, nor after the
    /// statistics were off. The tick that takes one sets no last activity, since what
    /// ran before it was not seen to run.
    seeded: bool,
    /// The running set's digest and each rule's definition hash, recomputed on a reload.
    defs_of: String,
    defs: HashMap<Arc<str>, String>,
}

fn matched(c: &RuleCounts) -> u64 {
    c.passed + c.no_result + c.failed
}

/// A rule's counts as `$SYS` and the admin API show them, `matched` included.
#[must_use]
pub fn counts_json(c: &RuleCounts) -> Value {
    json!({
        "matched": matched(c),
        "passed": c.passed,
        "no_result": c.no_result,
        "failed": c.failed,
        "actions_ok": c.actions_ok,
        "actions_failed": c.actions_failed,
    })
}

/// Per-second rates from `before` to `now` over `secs`, to the thousandth.
#[allow(clippy::cast_precision_loss)] // counter growth per tick, far below 2^52
fn rates_json(now: &RuleCounts, before: Option<&RuleCounts>, secs: f64) -> Value {
    let rate = |n: u64, b: u64| {
        if secs > 0.0 && before.is_some() {
            (n.saturating_sub(b) as f64 / secs * 1000.0).round() / 1000.0
        } else {
            0.0
        }
    };
    let b = before.copied().unwrap_or_default();
    json!({
        "matched": rate(matched(now), matched(&b)),
        "passed": rate(now.passed, b.passed),
        "no_result": rate(now.no_result, b.no_result),
        "failed": rate(now.failed, b.failed),
        "actions_ok": rate(now.actions_ok, b.actions_ok),
        "actions_failed": rate(now.actions_failed, b.actions_failed),
    })
}

/// Keep a `delivery` last error for `rule` when its failed actions grew since the last
/// tick: a refused or unrouted derived message is counted where it happens, with no text
/// to keep. An error stored since the last tick explains the growth instead — or a
/// render failure still inside its once-per-interval report gate, which keeps failing
/// without being stored again.
fn note_delivery_failures(
    observe: &RulesObserve,
    rule: &Arc<str>,
    failed: u64,
    prev_tick: SystemTime,
    at: SystemTime,
    def: &str,
) {
    let explained = observe.last_error(rule).is_some_and(|e| {
        e.kind != ErrorKind::Delivery
            && e.at + Duration::from_secs(FAILURE_WARN_INTERVAL_SECS) >= prev_tick
    });
    if failed > 0 && !explained {
        observe.set_last_error(
            rule,
            LastError {
                at,
                kind: ErrorKind::Delivery,
                message: format!("{failed} derived message(s) failed (refused or not routed)"),
                def: def.to_string(),
            },
        );
    }
}

impl Stats {
    /// One tick's messages, summary first.
    fn tick(&mut self, ctx: &Tick<'_>, now: Instant, at: SystemTime) -> Vec<(String, String)> {
        let Some(observe) = ctx.rules.observe() else {
            return Vec::new();
        };
        let set = ctx.rules.current();
        if self.defs_of != set.digest() {
            self.defs = set
                .rules()
                .iter()
                .map(|r| (r.id().clone(), rule_def(r)))
                .collect();
            set.digest().clone_into(&mut self.defs_of);
        }
        // A kept error about a rule a reload removed or redefined is no longer its own.
        observe.retain_errors(|id, e| self.defs.get(id) == Some(&e.def));
        let secs = self
            .prev_at
            .map_or(0.0, |(then, _)| now.duration_since(then).as_secs_f64());
        let seeding = !std::mem::replace(&mut self.seeded, true);
        if seeding {
            // The ids kept from before are seen afresh too, so one put back later is
            // measured from now.
            for (id, count) in &mut self.seen {
                *count = ctx.metrics.map_or(0, |m| matched(&m.rule_counts(id)));
            }
        }
        let at_s = rfc3339_millis(at);
        let mut messages = vec![(
            format!("$SYS/brokers/{}/rules", self.node),
            self.summary(ctx, &set, observe, &at_s),
        )];
        let mut counted = HashMap::with_capacity(set.len());
        for rule in set.rules() {
            let id = rule.id();
            let def = self.defs.get(id).cloned().unwrap_or_default();
            let counts = ctx.metrics.map(|m| m.rule_counts(id)).unwrap_or_default();
            let before = self.prev.get(id);
            // Growth since the rule was last looked at; a rule never looked at (added
            // since the baseline) is measured from zero. Kept with the rules, where the
            // admin API reads it too.
            let now_matched = matched(&counts);
            let was = self.seen.insert(id.clone(), now_matched);
            if !seeding && now_matched > was.unwrap_or(0) {
                observe.set_last_active(id, at);
            }
            if let (Some(b), Some((_, prev_tick))) = (before, self.prev_at) {
                let failed = counts.actions_failed.saturating_sub(b.actions_failed);
                note_delivery_failures(observe, id, failed, prev_tick, at, &def);
            }
            // Never the text, which can quote a payload value, trace on or off.
            let last_error = observe
                .last_error(id)
                .map(|e| json!({"at": rfc3339_millis(e.at), "kind": e.kind.as_str()}));
            let doc = json!({
                "node": self.node,
                "rule": &**id,
                "at": at_s,
                "enabled": rule.enabled(),
                "def": def,
                "counts": counts_json(&counts),
                "rates": rates_json(&counts, before, secs),
                "last_active_at": observe.last_active(id).map(rfc3339_millis),
                "last_error": last_error,
            });
            messages.push((
                format!("$SYS/brokers/{}/rules/{id}", self.node),
                doc.to_string(),
            ));
            counted.insert(id.clone(), counts);
        }
        self.prev = counted;
        if self.seen.len() > SEEN_MAX {
            self.seen.retain(|id, _| self.prev.contains_key(id));
        }
        observe.retain_last_active(|id| self.seen.contains_key(id));
        self.prev_at = Some((now, at));
        messages
    }

    /// The summary: the node, the running set, the settings, what was dropped, and the
    /// last reload's outcome — its failing part, never its text.
    fn summary(&self, ctx: &Tick<'_>, set: &RuleSet, observe: &RulesObserve, at_s: &str) -> String {
        let settings = observe.settings();
        let reload = ctx.last_reload.get().map(|r| {
            json!({
                "at": rfc3339_millis(r.at),
                "trigger": r.trigger,
                "applied": r.applied,
                "error_kind": r.error_kind(),
                "repeats": r.repeats,
            })
        });
        json!({
            "node": self.node,
            "at": at_s,
            "started_at": self.started_at,
            "interval_secs": ctx.interval,
            "rules": set.len(),
            "enabled": crate::reload::enabled_rules(set),
            "digest": set.digest(),
            "trace": settings.trace,
            "trace_rate": settings.trace_rate,
            "trace_dropped": observe.trace_dropped(),
            "stats_dropped": observe.stats_dropped(),
            "reload": reload,
        })
        .to_string()
    }
}

/// Publish the trace records `rx` receives until `shutdown` (ADR 0084 D5). Returns at
/// once when the rules are not watched.
pub async fn run_trace(
    mut rx: mpsc::Receiver<TraceRecord>,
    rules: Rules,
    hub: mpsc::UnboundedSender<HubCommand>,
    ingress: Arc<IngressCredit>,
    shutdown: CancellationToken,
) {
    let Some(observe) = rules.observe().cloned() else {
        return;
    };
    let node = rules.node().to_string();
    // The node-wide ceiling: (start of the current second, records sent in it).
    let mut window = (Instant::now(), 0u32);
    loop {
        let record = tokio::select! {
            () = shutdown.cancelled() => return,
            r = rx.recv() => match r {
                Some(r) => r,
                None => return,
            },
        };
        observe.released(record.weight());
        // Turned off since it was queued: what the operator turned off stays off.
        if !observe.tracing() {
            continue;
        }
        let now = Instant::now();
        if now.duration_since(window.0) >= Duration::from_secs(1) {
            window = (now, 0);
        }
        if window.1 >= observe.trace_rate().max(TRACE_NODE_FLOOR) {
            observe.count_trace_dropped();
            continue;
        }
        window.1 += 1;
        let topic = format!("$SYS/brokers/{node}/trace/rules/{}", record.rule);
        let payload = record_json(&node, &record).to_string();
        if !sys_publish(&hub, &ingress, topic, payload, SYS_EXPIRY_SECS) {
            observe.count_trace_dropped();
        }
    }
}

/// A payload as JSON fields: the kept bytes as UTF-8 text when they are (a character cut
/// at the end of a kept prefix still is), base64 otherwise, with the whole length and
/// whether only part was kept.
fn payload_fields(p: &TracePayload, out: &mut Map<String, Value>) {
    let (text, encoding) = match std::str::from_utf8(&p.bytes) {
        Ok(s) => (s.to_string(), "utf8"),
        Err(e) if p.truncated() && e.error_len().is_none() => (
            String::from_utf8_lossy(&p.bytes[..e.valid_up_to()]).into_owned(),
            "utf8",
        ),
        Err(_) => (crate::backup::b64_encode(&p.bytes), "base64"),
    };
    out.insert("payload".into(), json!(text));
    out.insert("payload_encoding".into(), json!(encoding));
    out.insert("payload_bytes".into(), json!(p.len));
    out.insert("truncated".into(), json!(p.truncated()));
}

fn message_json(kind: &str, m: &TracedMessage) -> Value {
    let mut out = Map::new();
    out.insert("type".into(), json!(kind));
    out.insert("topic".into(), json!(m.topic));
    out.insert("qos".into(), json!(m.qos));
    out.insert("retain".into(), json!(m.retain));
    out.insert("clientid".into(), json!(m.clientid));
    out.insert("username".into(), json!(m.username));
    payload_fields(&m.payload, &mut out);
    Value::Object(out)
}

fn trigger_json(t: &TraceTrigger) -> Value {
    match t {
        TraceTrigger::Publish(m) => message_json("publish", m),
        TraceTrigger::Will(m) => message_json("will", m),
        TraceTrigger::Event {
            event,
            clientid,
            username,
        } => json!({
            "type": "event",
            "event": event,
            "clientid": clientid,
            "username": username,
        }),
    }
}

/// One rendered output as a trace record shows it — and as the admin API's dry run
/// shows what a rule would render.
#[must_use]
pub fn output_json(o: &TraceOutput) -> Value {
    match o {
        TraceOutput::Republish {
            topic,
            qos,
            retain,
            payload,
        } => {
            let mut out = Map::new();
            out.insert("action".into(), json!("republish"));
            out.insert("topic".into(), json!(topic));
            out.insert("qos".into(), json!(qos));
            out.insert("retain".into(), json!(retain));
            payload_fields(payload, &mut out);
            Value::Object(out)
        }
        // The selected fields, as the JSON they are; cut ones (past 1 KiB) as text.
        TraceOutput::Console { output, len } if output.len() == *len => {
            match serde_json::from_str::<Value>(output) {
                Ok(v) => json!({"action": "console", "output": v}),
                Err(_) => json!({"action": "console", "output": output}),
            }
        }
        TraceOutput::Console { output, len } => json!({
            "action": "console",
            "output": output,
            "output_bytes": len,
            "truncated": true,
        }),
        TraceOutput::Failed {
            action_index,
            error,
        } => json!({"action_index": action_index, "error": error}),
    }
}

/// A trace record as the JSON published for it.
#[must_use]
pub fn record_json(node: &str, r: &TraceRecord) -> Value {
    json!({
        "node": node,
        "rule": &*r.rule,
        "at": rfc3339_millis(r.at),
        "trigger": trigger_json(&r.trigger),
        "result": r.result.as_str(),
        "error": r.error,
        "outputs": r.outputs.iter().map(output_json).collect::<Vec<_>>(),
        "outputs_omitted": r.outputs_omitted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Times on `$SYS` are RFC 3339 in UTC with milliseconds, whatever the host's zone.
    #[test]
    fn times_are_rfc3339_utc_with_milliseconds() {
        assert_eq!(rfc3339_millis(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        let t = UNIX_EPOCH + Duration::from_millis(1_775_000_000_123);
        assert_eq!(rfc3339_millis(t), "2026-03-31T23:33:20.123Z");
        let leap = UNIX_EPOCH + Duration::from_millis(951_782_400_999);
        assert_eq!(rfc3339_millis(leap), "2000-02-29T00:00:00.999Z");
    }

    /// A kept payload is UTF-8 text when it is, a cut through a character at the end of
    /// a truncated copy included, and base64 otherwise.
    #[test]
    fn a_payload_is_text_when_it_is_and_base64_when_not() {
        let fields = |bytes: &'static [u8], len: usize| {
            let mut out = Map::new();
            payload_fields(
                &TracePayload {
                    bytes: Bytes::from_static(bytes),
                    len,
                },
                &mut out,
            );
            Value::Object(out)
        };
        let v = fields(b"{\"t\":1}", 7);
        assert_eq!(v["payload"], "{\"t\":1}");
        assert_eq!(v["payload_encoding"], "utf8");
        assert_eq!(v["truncated"], false);
        // "é" is two bytes; a copy that ends between them is still text.
        let v = fields(b"ab\xC3", 4);
        assert_eq!(
            (v["payload"].as_str(), v["payload_encoding"].as_str()),
            (Some("ab"), Some("utf8"))
        );
        assert_eq!(
            (v["payload_bytes"].as_u64(), v["truncated"].as_bool()),
            (Some(4), Some(true))
        );
        let v = fields(b"\x00\xFF\x10", 3);
        assert_eq!(v["payload"], "AP8Q");
        assert_eq!(v["payload_encoding"], "base64");
    }
}
