//! Watching the running rules on `$SYS` ([ADR 0084](../../../docs/adr/0084-watching-and-editing-rules-live.md)):
//! the per-rule statistics.
//!
//! Spawned at boot and stopped by the shutdown token, which the broker cancels right
//! after telling the hub it is draining:
//!
//! - [`run_stats`] publishes, every `[rules] sys_interval_secs`, a summary on
//!   `$SYS/brokers/<node>/rules` and one message per rule — enabled or not — on
//!   `$SYS/brokers/<node>/rules/<id>`. The counts are the Prometheus counters read back
//!   without creating a series, cumulative since the broker started and keyed by rule
//!   id; the rates are their growth over the measured time between two ticks. An
//!   interval change applies at once.
//!
//! Every message is `QoS` 0, never retained, routed by [`HubCommand::SysPublish`], and
//! takes node-pool ingress credit first (ADR 0082): when the pool is short the rest of a
//! statistics tick is skipped and counted, so the statistics yield to clients under
//! pressure. Nothing secret goes on `$SYS`: no SQL, description, actions, file path,
//! writer or reload error text, and a rule's last error text only while the trace —
//! which shows payloads anyway — is on.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mqtt_observability::metrics::{Metrics, RuleCounts};
use mqtt_rules::RuleSet;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::hub::HubCommand;
use crate::ingress::IngressCredit;
use crate::reload::LastReload;
use crate::rules::{
    rule_def, ErrorKind, LastError, Rules, RulesObserve, FAILURE_WARN_INTERVAL_SECS,
};

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

/// Send one `$SYS` message, charged to the node pool; `false` when the pool is short.
fn sys_publish(
    hub: &mpsc::UnboundedSender<HubCommand>,
    ingress: &IngressCredit,
    topic: String,
    payload: String,
) -> bool {
    let Some(permit) = ingress.try_acquire_pool(ingress.cost(topic.len(), payload.len())) else {
        return false;
    };
    let _ = hub.send(HubCommand::SysPublish {
        topic,
        payload: Bytes::from(payload),
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
                for (topic, payload) in stats.tick(&ctx, now, SystemTime::now()) {
                    if !sys_publish(&hub, &ingress, topic, payload) {
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

/// What the statistics remember from one tick to the next.
#[derive(Default)]
struct Stats {
    node: String,
    started_at: String,
    /// Each rule's counts at the previous tick, and when it was.
    prev: HashMap<Arc<str>, RuleCounts>,
    prev_at: Option<(Instant, SystemTime)>,
    /// When each rule's `matched` last grew.
    last_active: HashMap<Arc<str>, SystemTime>,
    /// The running set's digest and each rule's definition hash, recomputed on a reload.
    defs_of: String,
    defs: HashMap<Arc<str>, String>,
}

fn matched(c: &RuleCounts) -> u64 {
    c.passed + c.no_result + c.failed
}

fn counts_json(c: &RuleCounts) -> Value {
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
        let tracing = observe.tracing();
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
            if before.map_or(matched(&counts) > 0, |b| matched(&counts) > matched(b)) {
                self.last_active.insert(id.clone(), at);
            }
            if let (Some(b), Some((_, prev_tick))) = (before, self.prev_at) {
                let failed = counts.actions_failed.saturating_sub(b.actions_failed);
                note_delivery_failures(observe, id, failed, prev_tick, at, &def);
            }
            let last_error = observe.last_error(id).map(|e| {
                let mut v = json!({"at": rfc3339_millis(e.at), "kind": e.kind.as_str()});
                // The text can quote a payload value: on $SYS only while the trace, which
                // shows payloads anyway, is on.
                if tracing {
                    v["message"] = json!(e.message);
                }
                v
            });
            let doc = json!({
                "node": self.node,
                "rule": &**id,
                "at": at_s,
                "enabled": rule.enabled(),
                "def": def,
                "counts": counts_json(&counts),
                "rates": rates_json(&counts, before, secs),
                "last_active_at": self.last_active.get(id).map(|t| rfc3339_millis(*t)),
                "last_error": last_error,
            });
            messages.push((
                format!("$SYS/brokers/{}/rules/{id}", self.node),
                doc.to_string(),
            ));
            counted.insert(id.clone(), counts);
        }
        self.prev = counted;
        self.last_active.retain(|id, _| self.prev.contains_key(id));
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
}
