//! The rule engine on the publish path ([ADR 0083](../../../docs/adr/0083-rule-engine.md)).
//!
//! `mqtt-rules` evaluates; this module decides where and what the results mean:
//!
//! - **Where.** A client's publish is evaluated on the **connection task** that read
//!   it, after the ACL and before the hub. Connection tasks run in parallel across the
//!   runtime's workers and each publish is evaluated once, on the node it arrived at,
//!   so rule work scales with connections and with nodes exactly as ingress does — the
//!   single-threaded hub never runs SQL for client publishes. A forwarded copy arriving
//!   from a peer is never re-evaluated: its landing node already did. Wills are the one
//!   exception (the hub publishes them, so the hub evaluates them; they are rare).
//! - **What a republish is.** An ordinary publish into the hub — routed, forwarded,
//!   retained, queued for offline sessions and quorum-replicated exactly like a client
//!   one — but with **no publisher** (so No Local does not apply, as in EMQX where the
//!   rule is the sender) and **never re-evaluated** by the rule engine, so no rule can
//!   loop (EMQX's `direct_dispatch`, always on).
//! - **When it is published.** A publish and what its rules derived travel to the hub
//!   as ONE command ([`HubCommand::PublishBatch`]). The hub routes the original first
//!   and routes the derived messages only if it accepted the original, so a refused
//!   publish leaves nothing behind for its resend to duplicate.
//! - **`QoS`.** For an inbound `QoS` 1/2 publish, every `QoS` ≥ 1 message its rules
//!   produce gets its own acknowledgement gate, and the publisher's PUBACK/PUBREC waits
//!   for all of them ([`join_outcomes`]) — when it is released, each derived message
//!   was stored where it was owed or has failed and been counted. The answer itself is
//!   exactly the **original's**: a derived message never changes what the publisher is
//!   told, because withholding an original that was already delivered — to retry a
//!   derived message the broker refused, or could not vouch for — re-delivers it on
//!   every retry for as long as the condition lasts (a brownout, a peer's refusal).
//!   Inbound `QoS` 2 dedup means a rule fires exactly once per `QoS` 2 message. A `QoS`
//!   0 publish has no acknowledgement, so nothing it produces is gated.
//! - **Watching it** ([ADR 0084](../../../docs/adr/0084-watching-and-editing-rules-live.md)).
//!   [`RulesObserve`] holds the live `[rules]` settings for the `$SYS` statistics and
//!   the trace, each rule's last error, and the trace's queue. With the trace off an
//!   evaluation pays one relaxed load for it; on, each rule's evaluations are copied —
//!   capped, rate-limited per rule — into plain [`TraceRecord`]s, and the trace task
//!   ([`crate::rules_sys`]) turns them into JSON off the connection tasks and the hub.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mqtt_codec::QoS;
use mqtt_core::{AppProperties, ClientId};
use mqtt_observability::metrics::Metrics;
use mqtt_rules::{
    ClientInfo, Effect, EventInput, Input, Outcome, PublishInput, Republish, Rule, RuleSet,
};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, warn};

use crate::hub::{DerivedPublish, HubCommand, PublishBatch, PublishOutcome};

/// The live rule set, swapped by a reload (ADR 0032 validate-before-swap).
pub type RulesWatch = watch::Receiver<Arc<RuleSet>>;

/// The rule engine as the broker uses it: the live rules, this node's id (the `node`
/// field), the metrics they report into and, when they are watched, the
/// [`RulesObserve`] (ADR 0084). Shared by every connection; each one evaluates through
/// its own [`ConnRules`].
#[derive(Clone)]
pub struct Rules {
    rx: RulesWatch,
    node: Arc<str>,
    metrics: Option<Arc<Metrics>>,
    observe: Option<Arc<RulesObserve>>,
}

impl std::fmt::Debug for Rules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rules")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

/// Who published (or connected), as far as a rule can see.
#[derive(Debug, Clone, Default)]
pub struct Publisher {
    /// The CONNECT username, when one was sent.
    pub username: Option<String>,
    /// The client's address — `None` for a relocated session, whose socket peer is
    /// the relaying node, not the client (ADR 0005).
    pub peer: Option<SocketAddr>,
}

/// The facts of one publish a rule evaluates.
#[derive(Debug, Clone, Copy)]
pub struct PublishFacts<'a> {
    /// The publishing client.
    pub client: &'a ClientId,
    /// Its username and address.
    pub publisher: &'a Publisher,
    /// Topic (aliases resolved).
    pub topic: &'a str,
    /// Payload.
    pub payload: &'a Bytes,
    /// Publish `QoS`.
    pub qos: QoS,
    /// RETAIN flag.
    pub retain: bool,
    /// DUP flag.
    pub dup: bool,
    /// MQTT 5 application properties.
    pub app: &'a AppProperties,
    /// MQTT 5 Message Expiry Interval.
    pub message_expiry: Option<u32>,
}

/// A message a rule produced, and the rule that produced it (its action's metric is
/// counted once the message's fate is known).
#[derive(Debug)]
pub struct Derived {
    rule: Arc<str>,
    msg: Republish,
}

impl Derived {
    /// Its topic length, and its payload-plus-properties length: what its ingress
    /// credit is charged on (ADR 0082 T3).
    #[must_use]
    pub fn accounted(&self) -> (usize, usize) {
        (
            self.msg.topic.len(),
            self.msg.payload.len() + self.msg.app.accounted_bytes(),
        )
    }
}

fn qos_num(q: QoS) -> u8 {
    match q {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn qos_of(n: u8) -> QoS {
    match n {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

/// A failing rule fails on every message it sees: the metric counts each failure, the
/// log says so once per interval per rule ([`Rule::failure_report_due`]).
pub(crate) const FAILURE_WARN_INTERVAL_SECS: u64 = 10;

/// Whether this failure of `rule` is the one to log at WARN.
fn warn_due(rule: &Rule) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    rule.failure_report_due(now, FAILURE_WARN_INTERVAL_SECS)
}

/// Count what evaluation reports. A successful action is NOT counted here: a console
/// action is counted when it logs and a republish once its fate is known
/// ([`count_action`]), so `result="ok"` means the message was routed, not merely
/// rendered.
///
/// The failure logged at WARN is also the rule's last error (ADR 0084): stored at the
/// same once-per-interval gate, so a failing rule costs one lock and one hash per
/// interval, not per message.
fn report(
    metrics: Option<&Metrics>,
    observe: Option<&RulesObserve>,
    rule: &Rule,
    outcome: Outcome<'_>,
) {
    let (eval, action) = match outcome {
        Outcome::Passed => (Some("passed"), None),
        Outcome::NoResult => (Some("no_result"), None),
        Outcome::Failed(e) => {
            if warn_due(rule) {
                warn!(rule = %rule.id(), error = %e,
                      "rule SQL failed (counted in mqttd_rule_evaluations_total{{result=\"failed\"}}; \
                       this rule's further failures within {FAILURE_WARN_INTERVAL_SECS}s are logged at debug)");
                if let Some(o) = observe {
                    o.store_error(rule, ErrorKind::Sql, &e.to_string());
                }
            } else {
                debug!(rule = %rule.id(), error = %e, "rule SQL failed");
            }
            (Some("failed"), None)
        }
        Outcome::ActionOk => (None, None),
        Outcome::ActionFailed(e) => {
            if warn_due(rule) {
                warn!(rule = %rule.id(), error = %e,
                      "rule action failed (counted in mqttd_rule_actions_total{{result=\"failed\"}}; \
                       this rule's further failures within {FAILURE_WARN_INTERVAL_SECS}s are logged at debug)");
                if let Some(o) = observe {
                    o.store_error(rule, ErrorKind::Action, &e.to_string());
                }
            } else {
                debug!(rule = %rule.id(), error = %e, "rule action failed");
            }
            (None, Some("failed"))
        }
    };
    if let Some(m) = metrics {
        if let Some(r) = eval {
            m.rule_evaluated(rule.id(), r);
        }
        if let Some(r) = action {
            m.rule_action(rule.id(), r);
        }
    }
}

fn count_action(metrics: Option<&Metrics>, rule: &str, result: &'static str) {
    if let Some(m) = metrics {
        m.rule_action(rule, result);
    }
}

/// Turn effects into derived messages, logging (and counting) the console ones.
fn collect(metrics: Option<&Metrics>, effects: Vec<(Arc<str>, Effect)>) -> Vec<Derived> {
    effects
        .into_iter()
        .filter_map(|(rule, e)| match e {
            Effect::Republish(msg) => Some(Derived { rule, msg }),
            Effect::Console(json) => {
                info!(rule = %rule, output = %json, "rule console action");
                count_action(metrics, &rule, "ok");
                None
            }
        })
        .collect()
}

/// What a publish evaluation was fired by, so the trace can tell a Will from a client
/// publish (ADR 0084). Events go through [`ConnRules::fire_event`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerKind {
    /// A client's PUBLISH.
    Publish,
    /// A Will the hub publishes for a client.
    Will,
}

/// Evaluate a publish against `set`.
fn evaluate(
    set: &RuleSet,
    node: &str,
    metrics: Option<&Metrics>,
    observe: Option<&RulesObserve>,
    f: &PublishFacts<'_>,
    trigger: TriggerKind,
) -> Vec<Derived> {
    if !set.has_message_rules() {
        return Vec::new();
    }
    let mut input = PublishInput::new(&f.client.0, f.topic, f.payload, qos_num(f.qos), f.app);
    input.username = f.publisher.username.as_deref();
    input.peer = f.publisher.peer;
    input.retain = f.retain;
    input.dup = f.dup;
    input.message_expiry = f.message_expiry;
    input.node = node;
    let mut effects = Vec::new();
    // The trace's gate: one relaxed load, then the plain report or the capturing one.
    match observe.filter(|o| o.tracing()) {
        None => set.on_publish(
            &input,
            &mut |r, o| report(metrics, observe, r, o),
            &mut effects,
        ),
        Some(o) => {
            let mut capture = Capture::new(o.trace_rate());
            set.on_publish(
                &input,
                &mut |r, out| {
                    capture.see(r, out);
                    report(metrics, observe, r, out);
                },
                &mut effects,
            );
            capture.finish(o, &effects, || TraceTrigger::message(trigger, f));
        }
    }
    collect(metrics, effects)
}

impl Rules {
    /// The engine over a live rule set.
    #[must_use]
    pub fn new(rx: RulesWatch, node: Arc<str>, metrics: Option<Arc<Metrics>>) -> Self {
        Self {
            rx,
            node,
            metrics,
            observe: None,
        }
    }

    /// The same engine, watched (ADR 0084): its last errors are kept and, while the
    /// trace is on, its evaluations are traced into `observe`.
    #[must_use]
    pub fn with_observe(mut self, observe: Arc<RulesObserve>) -> Self {
        self.observe = Some(observe);
        self
    }

    /// What watches these rules, if anything does.
    #[must_use]
    pub fn observe(&self) -> Option<&Arc<RulesObserve>> {
        self.observe.as_ref()
    }

    /// This node's id, as the rules see it (their `node` field) and as `$SYS` names it.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The metrics the rules report into.
    #[must_use]
    pub fn metrics(&self) -> Option<&Arc<Metrics>> {
        self.metrics.as_ref()
    }

    /// Send what an event or a Will derived — there is no publisher to answer — as
    /// [`HubCommand::RuleDerived`]: ungated, so it holds no pending-publish entry, and
    /// counted by the hub as it routes it (`ok`) or refuses it (`failed`). A graceful
    /// shutdown waits for these to be routed and stored ([`HubCommand::Drained`]).
    fn send_derived(derived: Vec<Derived>, send: impl Fn(HubCommand)) {
        for d in derived {
            send(HubCommand::RuleDerived(Box::new(DerivedPublish {
                rule: d.rule,
                publish: derived_command(d.msg, None),
                gated: false,
            })));
        }
    }

    /// The rules in force now.
    #[must_use]
    pub fn current(&self) -> Arc<RuleSet> {
        self.rx.borrow().clone()
    }

    /// One connection's view of the rules (see [`ConnRules`]).
    #[must_use]
    pub fn for_connection(&self) -> ConnRules {
        let mut rx = self.rx.clone();
        let set = rx.borrow_and_update().clone();
        ConnRules {
            engine: self.clone(),
            view: Mutex::new(View { rx, set }),
        }
    }

    /// Evaluate a Will the hub is publishing and hand `send` the commands for what its
    /// rules produce ([`Rules::send_derived`]).
    pub fn on_will(&self, f: &PublishFacts<'_>, send: impl Fn(HubCommand)) {
        let derived = evaluate(
            &self.current(),
            &self.node,
            self.metrics.as_deref(),
            self.observe.as_deref(),
            f,
            TriggerKind::Will,
        );
        Self::send_derived(derived, send);
    }
}

/// The rule set a connection evaluates against, refreshed only when a reload has
/// swapped it.
struct View {
    rx: RulesWatch,
    set: Arc<RuleSet>,
}

impl View {
    fn fresh(&mut self) -> &RuleSet {
        // `has_changed` loads the watch's version: no shared write per publish.
        if self.rx.has_changed().unwrap_or(false) {
            self.set = self.rx.borrow_and_update().clone();
        }
        &self.set
    }
}

/// One connection's handle on the rule engine.
///
/// Per connection on purpose: every publish reads the rule set, and reading it
/// through the shared watch (`borrow()` and an `Arc` clone) would be atomic
/// read-modify-writes on one cache line from every worker, on the path that scales
/// across all cores. Here a publish takes this connection's own, uncontended lock and
/// evaluates against a cached `Arc`, touching shared state only to load the watch's
/// version.
pub struct ConnRules {
    engine: Rules,
    view: Mutex<View>,
}

impl std::fmt::Debug for ConnRules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnRules")
            .field("node", &self.engine.node)
            .finish_non_exhaustive()
    }
}

impl ConnRules {
    fn with_set<T>(&self, f: impl FnOnce(&RuleSet) -> T) -> T {
        let mut view = self
            .view
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(view.fresh())
    }

    /// Evaluate a client publish. Returns the messages its rules republish, in rule
    /// then action order. Empty — and nearly free — when no rule selects messages.
    #[must_use]
    pub fn on_publish(&self, f: &PublishFacts<'_>) -> Vec<Derived> {
        let metrics = self.engine.metrics.as_deref();
        let observe = self.engine.observe.as_deref();
        self.with_set(|set| {
            evaluate(
                set,
                &self.engine.node,
                metrics,
                observe,
                f,
                TriggerKind::Publish,
            )
        })
    }

    /// Pick up a reloaded rule set now, releasing the superseded one this connection
    /// held — for a connection that is idle but alive (its PINGREQ), so a series of
    /// reloads does not keep a series of old rule sets in memory.
    pub fn refresh(&self) {
        self.with_set(|_| ());
    }

    /// Whether any rule selects `kind` (checked before building the event).
    #[must_use]
    pub fn wants(&self, kind: mqtt_rules::EventKind) -> bool {
        self.with_set(|set| set.wants_event(kind))
    }

    /// The `ClientInfo` a client/session event is built from.
    #[must_use]
    pub fn client_info<'a>(&'a self, client: &'a ClientId, p: &'a Publisher) -> ClientInfo<'a> {
        ClientInfo {
            clientid: &client.0,
            username: p.username.as_deref(),
            peer: p.peer,
            sockname: None,
            node: &self.engine.node,
        }
    }

    /// Evaluate a client/session event and publish what its rules produce
    /// ([`Rules::send_derived`]).
    pub fn fire_event(&self, input: &EventInput, hub: &mpsc::UnboundedSender<HubCommand>) {
        let metrics = self.engine.metrics.as_deref();
        let observe = self.engine.observe.as_deref();
        let derived = self.with_set(|set| {
            let mut effects = Vec::new();
            match observe.filter(|o| o.tracing()) {
                None => {
                    set.on_event(
                        input,
                        &mut |r, o| report(metrics, observe, r, o),
                        &mut effects,
                    );
                }
                Some(o) => {
                    let mut capture = Capture::new(o.trace_rate());
                    set.on_event(
                        input,
                        &mut |r, out| {
                            capture.see(r, out);
                            report(metrics, observe, r, out);
                        },
                        &mut effects,
                    );
                    capture.finish(o, &effects, || TraceTrigger::event(input));
                }
            }
            collect(metrics, effects)
        });
        Rules::send_derived(derived, |cmd| {
            let _ = hub.send(cmd);
        });
    }

    /// Drop derived messages unsent — a `QoS` 0 publish under `shed-qos0` found no
    /// credit for them — counting each as a failed action.
    pub fn shed(&self, derived: Vec<Derived>) {
        let metrics = self.engine.metrics.as_deref();
        for d in derived {
            count_action(metrics, &d.rule, "failed");
        }
        if let Some(m) = metrics {
            m.publish_dropped("hub-ingress");
        }
    }

    /// Build the one hub command carrying a publish and what its rules derived, and
    /// return it with the receiver the publisher's acknowledgement waits on and how many
    /// hub acknowledgement gates the batch holds — the original's, if gated, plus one
    /// per gated derived message — which is what the connection's ack pipeline is
    /// bounded by. The caller sends the command, now or once its ingress credit is
    /// there; the receiver resolves only after the hub has handled it.
    ///
    /// `original` is the publish command and `done` its gate's receiver (`None` for
    /// `QoS` 0). Each derived message at `QoS` ≥ 1 behind a gated original gets its own
    /// gate, and its action is counted when the gate answers; the hub counts the
    /// others as it routes or drops them. `credit` (the original's ingress permit) and
    /// `derived_credit` are held until the whole batch has been dispatched.
    #[must_use]
    pub fn build_batch(
        &self,
        original: HubCommand,
        done: Option<oneshot::Receiver<PublishOutcome>>,
        derived: Vec<Derived>,
        credit: Option<crate::ingress::IngressPermit>,
        derived_credit: Option<crate::ingress::IngressPermit>,
    ) -> (HubCommand, Option<oneshot::Receiver<PublishOutcome>>, usize) {
        let gated = done.is_some();
        let mut commands = Vec::with_capacity(derived.len());
        let mut answers: Vec<DerivedAnswer> = Vec::new();
        for d in derived {
            let gate = (gated && d.msg.qos > 0).then(|| {
                let (tx, rx) = oneshot::channel();
                answers.push((d.rule.clone(), rx));
                tx
            });
            commands.push(DerivedPublish {
                rule: d.rule,
                gated: gate.is_some(),
                publish: derived_command(d.msg, gate),
            });
        }
        let holds = answers.len() + usize::from(gated);
        let batch = HubCommand::PublishBatch(Box::new(PublishBatch {
            original,
            derived: commands,
            credit,
            derived_credit,
        }));
        let metrics = self.engine.metrics.clone();
        (
            batch,
            done.map(|rx| join_outcomes(rx, answers, metrics)),
            holds,
        )
    }
}

/// The hub command for one rule-produced message.
///
/// `v5: false` is deliberate: the v5 answer to a retained-quota overflow is to refuse
/// the publish, which for a derived message would refuse the *original's*
/// acknowledgement over a rule's retain flag; the v3.1.1 answer — deliver it live,
/// retain nothing — is the right one for a message no client sent.
#[must_use]
pub fn derived_command(r: Republish, done: Option<oneshot::Sender<PublishOutcome>>) -> HubCommand {
    HubCommand::Publish {
        topic: r.topic,
        payload: r.payload,
        qos: qos_of(r.qos),
        retain: r.retain,
        message_expiry: r.message_expiry,
        app: r.app,
        done,
        v5: false,
        publisher: None,
        credit: None,
    }
}

/// A gated derived message's slot in [`join_outcomes`]: its rule, and its gate.
pub type DerivedAnswer = (Arc<str>, oneshot::Receiver<PublishOutcome>);

/// One acknowledgement for a publish and the gated messages its rules produced. It
/// resolves once every gate has answered, with **exactly the original's answer**:
/// accepted, refused, or withheld when the original's own fate is unknown.
///
/// A derived message never changes it. One the broker accepted is counted `ok`; one
/// it refused (a brownout), never routed (behind a refused original), or cannot vouch
/// for (a failed durable write, or a peer refusing a copy after a local one was
/// stored, which the hub turns into a withhold) is counted `failed`. Withholding the
/// original instead, to have the publisher retry a derived message, re-delivers an
/// original that was already delivered — on every retry, for as long as a sticky
/// condition like a brownout lasts, and as a fresh `QoS` 2 sighting each time.
#[must_use]
pub fn join_outcomes(
    original: oneshot::Receiver<PublishOutcome>,
    derived: Vec<DerivedAnswer>,
    metrics: Option<Arc<Metrics>>,
) -> oneshot::Receiver<PublishOutcome> {
    if derived.is_empty() {
        return original;
    }
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        if let Some(outcome) = combine(original, derived, metrics.as_deref()).await {
            let _ = tx.send(outcome);
        }
        // `None`: dropping `tx` withholds.
    });
    rx
}

async fn combine(
    original: oneshot::Receiver<PublishOutcome>,
    derived: Vec<DerivedAnswer>,
    metrics: Option<&Metrics>,
) -> Option<PublishOutcome> {
    let answer = original.await.ok();
    for (rule, gate) in derived {
        let result = match gate.await {
            Ok(PublishOutcome::Accepted) => "ok",
            Ok(PublishOutcome::Refused(r)) => {
                debug!(rule = %rule, refusal = r.as_str(), "a derived message was refused");
                "failed"
            }
            Err(_) => {
                debug!(rule = %rule, "a derived message's fate is unknown (its gate closed)");
                "failed"
            }
        };
        count_action(metrics, &rule, result);
    }
    answer
}

/// The live `[rules]` settings for watching the running rules (ADR 0084).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RulesSysSettings {
    /// Seconds between the `$SYS` statistics; 0 = off.
    pub sys_interval_secs: u64,
    /// Whether the rule trace is on.
    pub trace: bool,
    /// Trace records per rule per second.
    pub trace_rate: u32,
}

impl Default for RulesSysSettings {
    fn default() -> Self {
        Self::from(&mqtt_config::Rules::default())
    }
}

impl From<&mqtt_config::Rules> for RulesSysSettings {
    fn from(c: &mqtt_config::Rules) -> Self {
        Self {
            sys_interval_secs: c.sys_interval_secs,
            trace: c.trace,
            trace_rate: c.trace_rate,
        }
    }
}

/// The longest error text kept, in bytes (ADR 0084): an error can quote a payload value,
/// so it is cut, on a character boundary.
pub const ERROR_TEXT_MAX: usize = 256;
/// The bytes of a payload a trace record copies.
pub const TRACE_PAYLOAD_MAX: usize = 1024;
/// The bytes of a topic, client id or username a trace record keeps.
pub const TRACE_NAME_MAX: usize = 256;
/// The outputs a trace record lists; the rest are only counted.
pub const TRACE_OUTPUTS_MAX: usize = 16;
/// The trace records queued for the trace task.
pub const TRACE_QUEUE: usize = 1024;
/// The bytes the queued trace records may hold between them.
pub const TRACE_QUEUE_BYTES: usize = 4 << 20;

/// `s` cut to at most `max` bytes, on a character boundary.
#[must_use]
pub fn clip(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// A rule definition's short hash (ADR 0084): 16 hex digits of the SHA-256 of its SQL,
/// its actions as JSON and its `enable` flag — what an edit that changes what the rule
/// does changes, so a stored error can be told from one about an earlier definition.
#[must_use]
pub fn rule_def(rule: &Rule) -> String {
    let actions = serde_json::to_string(rule.action_specs()).unwrap_or_default();
    let mut text = String::with_capacity(rule.sql().len() + actions.len() + 4);
    text.push_str(rule.sql());
    text.push('\0');
    text.push_str(&actions);
    text.push('\0');
    text.push(if rule.enabled() { '1' } else { '0' });
    let mut def = crate::reload::sha256_hex(text.as_bytes());
    def.truncate(16);
    def
}

/// What a rule's last error was about (ADR 0084).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Its SQL failed.
    Sql,
    /// An action failed as it rendered (a bad topic, a `$SYS` topic, a bad `qos`, …).
    Action,
    /// Derived messages were refused or not routed: synthesized by the statistics from
    /// the failed-action count, since those sites carry no error text.
    Delivery,
}

impl ErrorKind {
    /// The name the statistics and the admin API give it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::Action => "action",
            Self::Delivery => "delivery",
        }
    }
}

/// A rule's last error (ADR 0084).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastError {
    /// When.
    pub at: SystemTime,
    /// What about.
    pub kind: ErrorKind,
    /// The error, at most [`ERROR_TEXT_MAX`] bytes.
    pub message: String,
    /// The [`rule_def`] it was about: an entry about an earlier definition is dropped.
    pub def: String,
}

/// What watches the running rules (ADR 0084), shared by [`Rules`] and its tasks.
///
/// The settings are applied from the committed config only — at boot and from the
/// reload's commit hook, never from a candidate a reload may still reject.
#[derive(Debug)]
pub struct RulesObserve {
    trace: AtomicBool,
    trace_rate: AtomicU32,
    settings: watch::Sender<RulesSysSettings>,
    trace_tx: mpsc::Sender<TraceRecord>,
    /// The bytes the queued trace records hold ([`TRACE_QUEUE_BYTES`] at most).
    trace_bytes: AtomicUsize,
    trace_dropped: AtomicU64,
    stats_dropped: AtomicU64,
    last_errors: Mutex<HashMap<Arc<str>, LastError>>,
    /// When each rule's evaluations last grew, as the statistics saw it at a tick.
    last_active: Mutex<HashMap<Arc<str>, SystemTime>>,
}

impl RulesObserve {
    /// Everything off, and the receiving end of the trace queue for
    /// [`run_trace`](crate::rules_sys::run_trace).
    #[must_use]
    pub fn new() -> (Arc<Self>, mpsc::Receiver<TraceRecord>) {
        let settings = RulesSysSettings::default();
        let (trace_tx, trace_rx) = mpsc::channel(TRACE_QUEUE);
        let observe = Arc::new(Self {
            trace: AtomicBool::new(settings.trace),
            trace_rate: AtomicU32::new(settings.trace_rate),
            settings: watch::channel(settings).0,
            trace_tx,
            trace_bytes: AtomicUsize::new(0),
            trace_dropped: AtomicU64::new(0),
            stats_dropped: AtomicU64::new(0),
            last_errors: Mutex::new(HashMap::new()),
            last_active: Mutex::new(HashMap::new()),
        });
        (observe, trace_rx)
    }

    /// Apply the committed `[rules]` settings. `exposed` says why anyone may read the
    /// trace (no ACL file, an ACL whose default is allow), when that is so: with the
    /// trace on it is logged as `INSECURE:`.
    pub fn apply(&self, config: &mqtt_config::Rules, exposed: Option<&str>) {
        let new = RulesSysSettings::from(config);
        let was_tracing = self.trace.swap(new.trace, Relaxed);
        self.trace_rate.store(new.trace_rate, Relaxed);
        if new.trace && !was_tracing {
            warn!(
                rate = new.trace_rate,
                "rule trace is ON: every rule's evaluations — trigger topic, client id, \
                 username, up to 1 KiB of payload, rendered outputs — are copied onto \
                 $SYS/brokers/<node>/trace/rules/<id> (ADR 0084); a subscriber there reads \
                 every topic that rule's FROM matches"
            );
        } else if !new.trace && was_tracing {
            info!("rule trace is off (ADR 0084)");
        }
        if let (true, Some(why)) = (new.trace, exposed) {
            warn!(
                "INSECURE: the rule trace is on and {why}: any client can subscribe to \
                 $SYS/brokers/+/trace/rules/+ and read what the rules see (ADR 0084)"
            );
        }
        self.settings.send_if_modified(|s| {
            let changed = *s != new;
            *s = new;
            changed
        });
    }

    /// The settings now in force.
    #[must_use]
    pub fn settings(&self) -> RulesSysSettings {
        *self.settings.borrow()
    }

    /// Follow the settings: a receiver told of each change.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<RulesSysSettings> {
        self.settings.subscribe()
    }

    /// Whether the trace is on: the one relaxed load an evaluation pays for it.
    #[must_use]
    pub fn tracing(&self) -> bool {
        self.trace.load(Relaxed)
    }

    /// Trace records per rule per second.
    #[must_use]
    pub fn trace_rate(&self) -> u32 {
        self.trace_rate.load(Relaxed)
    }

    /// Trace records dropped: the queue was full or over its byte budget, or the node
    /// ceiling or the ingress pool had no room to publish them.
    #[must_use]
    pub fn trace_dropped(&self) -> u64 {
        self.trace_dropped.load(Relaxed)
    }

    /// Statistics ticks skipped because the ingress pool had no room for them.
    #[must_use]
    pub fn stats_dropped(&self) -> u64 {
        self.stats_dropped.load(Relaxed)
    }

    pub(crate) fn count_trace_dropped(&self) {
        self.trace_dropped.fetch_add(1, Relaxed);
    }

    pub(crate) fn count_stats_dropped(&self) {
        self.stats_dropped.fetch_add(1, Relaxed);
    }

    /// `rule`'s last error, if one is kept.
    #[must_use]
    pub fn last_error(&self, rule: &str) -> Option<LastError> {
        self.errors().get(rule).cloned()
    }

    fn errors(&self) -> std::sync::MutexGuard<'_, HashMap<Arc<str>, LastError>> {
        self.last_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Keep `message` as `rule`'s last error.
    fn store_error(&self, rule: &Rule, kind: ErrorKind, message: &str) {
        let error = LastError {
            at: SystemTime::now(),
            kind,
            message: clip(message, ERROR_TEXT_MAX).to_string(),
            def: rule_def(rule),
        };
        self.errors().insert(rule.id().clone(), error);
    }

    /// Keep `error` as `rule`'s last error (the statistics' synthesized ones).
    pub(crate) fn set_last_error(&self, rule: &Arc<str>, error: LastError) {
        self.errors().insert(rule.clone(), error);
    }

    /// Drop the kept errors `keep` refuses: a rule gone, or redefined, by a reload.
    pub(crate) fn retain_errors(&self, keep: impl Fn(&str, &LastError) -> bool) {
        self.errors().retain(|id, e| keep(id, e));
    }

    /// When `rule` last ran, as of the statistics' last tick: the tick at which its
    /// evaluation count was seen to grow. `None` before it ran, and while the statistics
    /// are off.
    #[must_use]
    pub fn last_active(&self, rule: &str) -> Option<SystemTime> {
        self.active().get(rule).copied()
    }

    fn active(&self) -> std::sync::MutexGuard<'_, HashMap<Arc<str>, SystemTime>> {
        self.last_active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn set_last_active(&self, rule: &Arc<str>, at: SystemTime) {
        self.active().insert(rule.clone(), at);
    }

    /// Forget when the rules `keep` refuses last ran: a reload removed them.
    pub(crate) fn retain_last_active(&self, keep: impl Fn(&str) -> bool) {
        self.active().retain(|id, _| keep(id));
    }

    /// Queue a trace record, or drop and count it when the queue is full or the bytes
    /// queued would pass [`TRACE_QUEUE_BYTES`].
    fn offer(&self, record: TraceRecord) {
        let weight = record.weight();
        if self.trace_bytes.fetch_add(weight, Relaxed) + weight > TRACE_QUEUE_BYTES {
            self.trace_bytes.fetch_sub(weight, Relaxed);
            self.count_trace_dropped();
            return;
        }
        if self.trace_tx.try_send(record).is_err() {
            self.trace_bytes.fetch_sub(weight, Relaxed);
            self.count_trace_dropped();
        }
    }

    /// A record of `weight` left the queue.
    pub(crate) fn released(&self, weight: usize) {
        self.trace_bytes.fetch_sub(weight, Relaxed);
    }
}

/// How a traced evaluation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceResult {
    /// Its SQL produced output.
    Passed,
    /// Its `WHERE` filtered the message out.
    NoResult,
    /// Its SQL failed.
    Failed,
}

impl TraceResult {
    /// The name a trace record gives it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::NoResult => "no_result",
            Self::Failed => "failed",
        }
    }
}

/// A payload as a trace record keeps it: its first [`TRACE_PAYLOAD_MAX`] bytes, COPIED
/// (a slice would keep the whole message's allocation alive in the queue), and its
/// length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TracePayload {
    /// The copied bytes.
    pub bytes: Bytes,
    /// The payload's whole length.
    pub len: usize,
}

impl TracePayload {
    fn copy(payload: &[u8]) -> Self {
        Self {
            bytes: Bytes::copy_from_slice(&payload[..payload.len().min(TRACE_PAYLOAD_MAX)]),
            len: payload.len(),
        }
    }

    /// Whether only part of it was kept.
    #[must_use]
    pub fn truncated(&self) -> bool {
        self.bytes.len() < self.len
    }
}

/// The message a traced publish or Will evaluation ran on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TracedMessage {
    /// Its topic, at most [`TRACE_NAME_MAX`] bytes.
    pub topic: String,
    /// Its `QoS`.
    pub qos: u8,
    /// Its RETAIN flag.
    pub retain: bool,
    /// The publishing client, at most [`TRACE_NAME_MAX`] bytes.
    pub clientid: String,
    /// Its username, at most [`TRACE_NAME_MAX`] bytes.
    pub username: Option<String>,
    /// Its payload.
    pub payload: TracePayload,
}

/// What fired a traced evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceTrigger {
    /// A client's publish.
    Publish(TracedMessage),
    /// A client's Will.
    Will(TracedMessage),
    /// A client or session event.
    Event {
        /// The event's name (`client.connected`, …).
        event: &'static str,
        /// The client, at most [`TRACE_NAME_MAX`] bytes.
        clientid: String,
        /// Its username, at most [`TRACE_NAME_MAX`] bytes.
        username: Option<String>,
    },
}

fn name(s: &str) -> String {
    clip(s, TRACE_NAME_MAX).to_string()
}

impl TraceTrigger {
    fn message(kind: TriggerKind, f: &PublishFacts<'_>) -> Self {
        let message = TracedMessage {
            topic: name(f.topic),
            qos: qos_num(f.qos),
            retain: f.retain,
            clientid: name(&f.client.0),
            username: f.publisher.username.as_deref().map(name),
            payload: TracePayload::copy(f.payload),
        };
        match kind {
            TriggerKind::Publish => Self::Publish(message),
            TriggerKind::Will => Self::Will(message),
        }
    }

    fn event(input: &EventInput) -> Self {
        Self::Event {
            event: input.kind().event_name(),
            clientid: input
                .field("clientid")
                .as_str()
                .map(name)
                .unwrap_or_default(),
            username: input.field("username").as_str().map(name),
        }
    }

    fn weight(&self) -> usize {
        match self {
            Self::Publish(m) | Self::Will(m) => {
                m.topic.len()
                    + m.clientid.len()
                    + m.username.as_ref().map_or(0, String::len)
                    + m.payload.bytes.len()
            }
            Self::Event {
                clientid, username, ..
            } => clientid.len() + username.as_ref().map_or(0, String::len),
        }
    }
}

/// One thing a traced rule rendered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceOutput {
    /// A message to republish.
    Republish {
        /// Its topic, at most [`TRACE_NAME_MAX`] bytes.
        topic: String,
        /// Its `QoS`.
        qos: u8,
        /// Its RETAIN flag.
        retain: bool,
        /// Its payload.
        payload: TracePayload,
    },
    /// A `console` line: the selected fields as JSON, at most [`TRACE_PAYLOAD_MAX`]
    /// bytes of it.
    Console {
        /// The JSON (or its first bytes, when `len` is longer).
        output: String,
        /// The whole JSON's length.
        len: usize,
    },
    /// An action that failed as it rendered.
    Failed {
        /// Which of the rule's actions.
        action_index: usize,
        /// Why, at most [`ERROR_TEXT_MAX`] bytes.
        error: String,
    },
}

impl TraceOutput {
    fn of(effect: &Effect) -> Self {
        match effect {
            Effect::Republish(r) => Self::Republish {
                topic: name(&r.topic),
                qos: r.qos,
                retain: r.retain,
                payload: TracePayload::copy(&r.payload),
            },
            Effect::Console(json) => Self::Console {
                output: clip(json, TRACE_PAYLOAD_MAX).to_string(),
                len: json.len(),
            },
        }
    }

    fn weight(&self) -> usize {
        match self {
            Self::Republish { topic, payload, .. } => topic.len() + payload.bytes.len(),
            Self::Console { output, .. } => output.len(),
            Self::Failed { error, .. } => error.len(),
        }
    }
}

/// One traced evaluation of one rule (ADR 0084): a plain struct, built where the rule
/// ran — the connection task, or the hub for a Will — and turned into JSON only by the
/// trace task. Outputs are what the rule rendered; their fate is in the counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceRecord {
    /// The rule.
    pub rule: Arc<str>,
    /// When it ran.
    pub at: SystemTime,
    /// What it ran on.
    pub trigger: TraceTrigger,
    /// How its SQL ended.
    pub result: TraceResult,
    /// Its SQL's error, at most [`ERROR_TEXT_MAX`] bytes.
    pub error: Option<String>,
    /// What it rendered, at most [`TRACE_OUTPUTS_MAX`].
    pub outputs: Vec<TraceOutput>,
    /// The outputs past [`TRACE_OUTPUTS_MAX`].
    pub outputs_omitted: u32,
}

/// What a record costs beyond its texts and payload copies, for the byte budget.
const TRACE_RECORD_OVERHEAD: usize = 256;

impl TraceRecord {
    /// The bytes it holds, as the queue's byte budget counts them.
    #[must_use]
    pub fn weight(&self) -> usize {
        TRACE_RECORD_OVERHEAD
            + self.rule.len()
            + self.trigger.weight()
            + self.error.as_ref().map_or(0, String::len)
            + self.outputs.iter().map(TraceOutput::weight).sum::<usize>()
    }
}

/// One rule's slot in an evaluation being traced.
enum Slot {
    /// A rendered effect: its index in the evaluation's effects.
    Effect(usize),
    /// A failed action: which, and why.
    Failed(usize, String),
}

/// A rule whose evaluation is being recorded.
struct Pending {
    rule: Arc<str>,
    result: TraceResult,
    error: Option<String>,
    slots: Vec<Slot>,
    omitted: u32,
    /// Action reports seen so far (each output runs every action, in order).
    actions: usize,
    /// The rule's action count.
    per_output: usize,
}

impl Pending {
    fn add(&mut self, slot: impl FnOnce(usize) -> Slot) {
        let index = self.actions % self.per_output.max(1);
        self.actions += 1;
        if self.slots.len() < TRACE_OUTPUTS_MAX {
            self.slots.push(slot(index));
        } else {
            self.omitted = self.omitted.saturating_add(1);
        }
    }
}

/// The trace's view of one evaluation, fed by the report callback. The evaluation
/// reports each rule's result, then each action's (an `ActionOk` is followed by its
/// effect, in order), so the k-th `ActionOk` is the k-th effect.
struct Capture {
    at: SystemTime,
    now_s: u64,
    per_sec: u32,
    open: Option<Pending>,
    due: Vec<Pending>,
    effects: usize,
}

impl Capture {
    fn new(per_sec: u32) -> Self {
        let at = SystemTime::now();
        Self {
            at,
            now_s: at.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()),
            per_sec,
            open: None,
            due: Vec::new(),
            effects: 0,
        }
    }

    fn see(&mut self, rule: &Rule, outcome: Outcome<'_>) {
        let (result, error) = match outcome {
            Outcome::Passed => (TraceResult::Passed, None),
            Outcome::NoResult => (TraceResult::NoResult, None),
            Outcome::Failed(e) => (TraceResult::Failed, Some(e)),
            Outcome::ActionOk => {
                let effect = self.effects;
                self.effects += 1;
                if let Some(p) = &mut self.open {
                    p.add(|_| Slot::Effect(effect));
                }
                return;
            }
            Outcome::ActionFailed(e) => {
                if let Some(p) = &mut self.open {
                    p.add(|i| Slot::Failed(i, clip(&e.to_string(), ERROR_TEXT_MAX).to_string()));
                }
                return;
            }
        };
        self.due.extend(self.open.take());
        // Per rule, per second; `no_result` on a window of its own, so a rule whose WHERE
        // rarely passes does not spend its budget before the passes come.
        let due = match result {
            TraceResult::NoResult => rule.no_result_trace_due(self.now_s, self.per_sec),
            _ => rule.trace_due(self.now_s, self.per_sec),
        };
        if due {
            self.open = Some(Pending {
                rule: rule.id().clone(),
                result,
                error: error.map(|e| clip(&e.to_string(), ERROR_TEXT_MAX).to_string()),
                slots: Vec::new(),
                omitted: 0,
                actions: 0,
                per_output: rule.action_count(),
            });
        }
    }

    /// Turn what is due into records, with the trigger (built once, only if any is).
    fn finish(
        mut self,
        observe: &RulesObserve,
        effects: &[(Arc<str>, Effect)],
        trigger: impl FnOnce() -> TraceTrigger,
    ) {
        self.due.extend(self.open.take());
        if self.due.is_empty() {
            return;
        }
        let trigger = trigger();
        for p in self.due {
            let outputs = p
                .slots
                .into_iter()
                .filter_map(|slot| match slot {
                    Slot::Effect(i) => effects.get(i).map(|(_, e)| TraceOutput::of(e)),
                    Slot::Failed(action_index, error) => Some(TraceOutput::Failed {
                        action_index,
                        error,
                    }),
                })
                .collect();
            observe.offer(TraceRecord {
                rule: p.rule,
                at: self.at,
                trigger: trigger.clone(),
                result: p.result,
                error: p.error,
                outputs,
                outputs_omitted: p.omitted,
            });
        }
    }
}

/// Load the configured rules file. `None` path = no rules. Warnings are logged.
pub fn load(path: Option<&str>) -> Result<RuleSet, String> {
    let Some(path) = path else {
        return Ok(RuleSet::empty());
    };
    let loaded = RuleSet::load(std::path::Path::new(path)).map_err(|e| e.to_string())?;
    for w in &loaded.warnings {
        warn!(file = %path, "rules: {w}");
    }
    Ok(loaded.rules)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::PublishRefusal;

    fn answered(o: Option<PublishOutcome>) -> oneshot::Receiver<PublishOutcome> {
        let (tx, rx) = oneshot::channel();
        if let Some(o) = o {
            let _ = tx.send(o);
        }
        rx
    }

    async fn joined(
        first: Option<PublishOutcome>,
        rest: &[Option<PublishOutcome>],
    ) -> Option<PublishOutcome> {
        let rest = rest
            .iter()
            .map(|o| (Arc::from("r"), answered(*o)))
            .collect();
        join_outcomes(answered(first), rest, None).await.ok()
    }

    const OK: Option<PublishOutcome> = Some(PublishOutcome::Accepted);
    const REFUSED: Option<PublishOutcome> = Some(PublishOutcome::Refused(PublishRefusal::Brownout));
    const WITHHELD: Option<PublishOutcome> = None;

    /// The publisher hears exactly the original's answer, whatever its derived messages
    /// met: a derived failure is counted, never turned into a withhold that would have
    /// the publisher re-send — and the broker re-deliver — an original it already
    /// delivered.
    #[tokio::test]
    async fn the_publisher_hears_exactly_the_originals_answer() {
        for derived in [
            &[][..],
            &[OK, OK],
            &[REFUSED],
            &[OK, REFUSED],
            &[WITHHELD],
            &[OK, WITHHELD, REFUSED],
        ] {
            for original in [OK, REFUSED, WITHHELD] {
                assert_eq!(
                    joined(original, derived).await,
                    original,
                    "{original:?} with derived {derived:?}"
                );
            }
        }
    }

    fn action_count(m: &Metrics, result: &str) -> u64 {
        m.render()
            .lines()
            .find(|l| {
                l.starts_with("mqttd_rule_actions_total{")
                    && l.contains("rule=\"r\"")
                    && l.contains(&format!("result=\"{result}\""))
            })
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// A gated derived action is counted once its gate answers: accepted → `ok`;
    /// refused, never routed (its gate closed) or unknown → `failed`.
    #[tokio::test]
    async fn gated_derived_actions_are_counted_by_their_fate() {
        let metrics = Arc::new(Metrics::new("test"));
        let answers = vec![
            (Arc::from("r"), answered(OK)),
            (Arc::from("r"), answered(REFUSED)),
            (Arc::from("r"), answered(WITHHELD)),
        ];
        let got = join_outcomes(answered(OK), answers, Some(metrics.clone())).await;
        assert_eq!(got.ok(), OK);
        assert_eq!(action_count(&metrics, "ok"), 1, "the accepted one");
        assert_eq!(
            action_count(&metrics, "failed"),
            2,
            "the refused and the unknown one"
        );
    }
}
