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
//!   was stored where it was owed, or refused and counted. The answer itself is the
//!   **original's**: a derived message the broker refuses (brownout) fails its action,
//!   it never withholds an original that was already delivered. Only a derived
//!   message whose fate is unknown withholds, as an unknown fate does for any publish.
//!   Inbound `QoS` 2 dedup means a rule fires exactly once per `QoS` 2 message. A `QoS`
//!   0 publish has no acknowledgement, so nothing it produces is gated.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use mqtt_codec::QoS;
use mqtt_core::{AppProperties, ClientId};
use mqtt_observability::metrics::Metrics;
use mqtt_rules::{ClientInfo, Effect, EventInput, Outcome, PublishInput, Republish, Rule, RuleSet};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, warn};

use crate::hub::{HubCommand, PublishBatch, PublishOutcome};

/// The live rule set, swapped by a reload (ADR 0032 validate-before-swap).
pub type RulesWatch = watch::Receiver<Arc<RuleSet>>;

/// The rule engine as the broker uses it: the live rules, this node's id (the `node`
/// field) and the metrics they report into. Shared by every connection; each one
/// evaluates through its own [`ConnRules`].
#[derive(Clone)]
pub struct Rules {
    rx: RulesWatch,
    node: Arc<str>,
    metrics: Option<Arc<Metrics>>,
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

/// When the last rule failure was logged at WARN (unix seconds). A failing rule fails
/// on every message it sees; the metric counts each one, the log says so once per
/// interval.
static LAST_FAILURE_WARN: AtomicU64 = AtomicU64::new(0);
const FAILURE_WARN_INTERVAL_SECS: u64 = 10;

/// Count what evaluation reports. A successful action is NOT counted here: a console
/// action is counted when it logs and a republish once its fate is known
/// ([`count_action`]), so `result="ok"` means the message was routed, not merely
/// rendered.
fn report(metrics: Option<&Metrics>, rule: &Rule, outcome: Outcome<'_>) {
    let (eval, action) = match outcome {
        Outcome::Passed => (Some("passed"), None),
        Outcome::NoResult => (Some("no_result"), None),
        Outcome::Failed(e) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
            let last = LAST_FAILURE_WARN.load(Ordering::Relaxed);
            if now >= last + FAILURE_WARN_INTERVAL_SECS
                && LAST_FAILURE_WARN
                    .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                warn!(rule = %rule.id(), error = %e,
                      "rule SQL failed (counted in mqttd_rule_evaluations_total{{result=\"failed\"}}; \
                       further failures within {FAILURE_WARN_INTERVAL_SECS}s are logged at debug)");
            } else {
                debug!(rule = %rule.id(), error = %e, "rule SQL failed");
            }
            (Some("failed"), None)
        }
        Outcome::ActionOk => (None, None),
        Outcome::ActionFailed(e) => {
            debug!(rule = %rule.id(), error = %e, "rule action failed");
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

/// Evaluate a publish against `set`.
fn evaluate(
    set: &RuleSet,
    node: &str,
    metrics: Option<&Metrics>,
    f: &PublishFacts<'_>,
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
    set.on_publish(&input, &mut |r, o| report(metrics, r, o), &mut effects);
    collect(metrics, effects)
}

impl Rules {
    /// The engine over a live rule set.
    #[must_use]
    pub fn new(rx: RulesWatch, node: Arc<str>, metrics: Option<Arc<Metrics>>) -> Self {
        Self { rx, node, metrics }
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

    /// Evaluate a Will the hub is publishing and return the commands for what its
    /// rules produce: ungated, as the Will is (there is no publisher to answer).
    #[must_use]
    pub fn on_will(&self, f: &PublishFacts<'_>) -> Vec<HubCommand> {
        let metrics = self.metrics.as_deref();
        evaluate(&self.current(), &self.node, metrics, f)
            .into_iter()
            .map(|d| {
                count_action(metrics, &d.rule, "ok");
                derived_command(d.msg, None)
            })
            .collect()
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
        self.with_set(|set| evaluate(set, &self.engine.node, metrics, f))
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

    /// Evaluate a client/session event and publish what its rules produce. Events
    /// gate nothing — there is no publisher acknowledgement to hold — so the
    /// republishes go to the hub ungated, as a Will's do.
    pub fn fire_event(&self, input: &EventInput, hub: &mpsc::UnboundedSender<HubCommand>) {
        let metrics = self.engine.metrics.as_deref();
        let derived = self.with_set(|set| {
            let mut effects = Vec::new();
            set.on_event(input, &mut |r, o| report(metrics, r, o), &mut effects);
            collect(metrics, effects)
        });
        for d in derived {
            count_action(metrics, &d.rule, "ok");
            let _ = hub.send(derived_command(d.msg, None));
        }
    }

    /// Send a publish and what its rules derived to the hub as one batch. Returns the
    /// receiver the publisher's acknowledgement waits on, and how many hub
    /// acknowledgement gates the batch holds — the original's, if gated, plus one per
    /// gated derived message — which is what the connection's ack pipeline is bounded
    /// by.
    ///
    /// `original` is the publish command and `done` its gate's receiver (`None` for
    /// `QoS` 0). Each derived message at `QoS` ≥ 1 behind a gated original gets its own
    /// gate; `QoS` 0 ones promise nothing and are not waited for. `credit`, the
    /// original's ingress permit, is held until the whole batch has been dispatched.
    #[must_use]
    pub fn send_batch(
        &self,
        hub: &mpsc::UnboundedSender<HubCommand>,
        original: HubCommand,
        done: Option<oneshot::Receiver<PublishOutcome>>,
        derived: Vec<Derived>,
        credit: Option<crate::ingress::IngressPermit>,
    ) -> (Option<oneshot::Receiver<PublishOutcome>>, usize) {
        let metrics = self.engine.metrics.clone();
        let gated = done.is_some();
        let mut commands = Vec::with_capacity(derived.len());
        let mut answers: Vec<DerivedAnswer> = Vec::with_capacity(derived.len());
        for d in derived {
            let gate = if gated && d.msg.qos > 0 {
                let (tx, rx) = oneshot::channel();
                answers.push((d.rule, Some(rx)));
                Some(tx)
            } else {
                if gated {
                    answers.push((d.rule, None));
                } else {
                    // Nothing waits on a `QoS` 0 publish: handed to the hub is all that
                    // will ever be known.
                    count_action(metrics.as_deref(), &d.rule, "ok");
                }
                None
            };
            commands.push(derived_command(d.msg, gate));
        }
        let holds = answers.iter().filter(|(_, rx)| rx.is_some()).count() + usize::from(gated);
        let _ = hub.send(HubCommand::PublishBatch(Box::new(PublishBatch {
            original,
            derived: commands,
            credit,
        })));
        (done.map(|rx| join_outcomes(rx, answers, metrics)), holds)
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

/// A derived message's slot in [`join_outcomes`]: its rule, and its gate (`None` for
/// a `QoS` 0 derived message, which promises nothing).
pub type DerivedAnswer = (Arc<str>, Option<oneshot::Receiver<PublishOutcome>>);

/// One acknowledgement for a publish and the gated messages its rules produced. It
/// resolves once every gate has answered, and says:
///
/// - the original was **refused** → that refusal (the hub routed none of its derived
///   messages, so nothing was stored for it anywhere);
/// - the original's fate is **unknown** → withheld (the connection closes unacked and
///   the publisher retries, as for any publish);
/// - the original was **accepted** → accepted, unless a derived message's fate is
///   unknown, which withholds as an unknown original would. A derived message the
///   broker **refused** (a brownout, say) was decided before any side effect, so it is
///   counted as a failed action and the original is still acknowledged: withholding an
///   original that was already delivered would re-deliver it on every retry for as
///   long as the refusal lasts.
///
/// Each derived message's action is counted here, once its fate is known.
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
    let first = original.await;
    let original_refused = matches!(first, Ok(PublishOutcome::Refused(_)));
    let mut unknown = false;
    for (rule, gate) in derived {
        let result = match gate {
            // `QoS` 0: routed exactly when the original was not refused.
            None if original_refused => "failed",
            None => "ok",
            Some(rx) => match rx.await {
                Ok(PublishOutcome::Accepted) => "ok",
                Ok(PublishOutcome::Refused(_)) => "failed",
                // Never routed, because the original was refused, or routed with a
                // fate nobody can vouch for.
                Err(_) => {
                    unknown = true;
                    "failed"
                }
            },
        };
        count_action(metrics, &rule, result);
    }
    match first {
        Ok(PublishOutcome::Refused(r)) => Some(PublishOutcome::Refused(r)),
        Ok(PublishOutcome::Accepted) if !unknown => Some(PublishOutcome::Accepted),
        _ => None,
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
            .map(|o| (Arc::from("r"), Some(answered(*o))))
            .collect();
        join_outcomes(answered(first), rest, None).await.ok()
    }

    const OK: Option<PublishOutcome> = Some(PublishOutcome::Accepted);
    const REFUSED: Option<PublishOutcome> = Some(PublishOutcome::Refused(PublishRefusal::Brownout));
    const WITHHELD: Option<PublishOutcome> = None;

    #[tokio::test]
    async fn the_publisher_hears_the_originals_answer_unless_a_fate_is_unknown() {
        assert_eq!(
            joined(OK, &[]).await,
            OK,
            "no derived: the original's own answer"
        );
        assert_eq!(joined(OK, &[OK, OK]).await, OK);
        // A refused derived message stored nothing: count it and ack the original that
        // was delivered. Withholding would re-deliver it on every retry.
        assert_eq!(joined(OK, &[REFUSED]).await, OK);
        assert_eq!(joined(OK, &[OK, REFUSED]).await, OK);
        // A refused original: the hub routed none of its derived messages.
        assert_eq!(joined(REFUSED, &[WITHHELD]).await, REFUSED);
        assert_eq!(joined(REFUSED, &[REFUSED]).await, REFUSED);
        // An unknown fate anywhere withholds, as it does for any publish.
        assert_eq!(joined(OK, &[OK, WITHHELD]).await, WITHHELD);
        assert_eq!(joined(WITHHELD, &[OK]).await, WITHHELD);
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

    /// A derived action is counted once its fate is known: routed → `ok`; refused,
    /// skipped behind a refused original, or unknown → `failed`.
    #[tokio::test]
    async fn derived_actions_are_counted_by_their_fate() {
        let metrics = Arc::new(Metrics::new("test"));
        let answers = vec![
            (Arc::from("r"), Some(answered(OK))),
            (Arc::from("r"), Some(answered(REFUSED))),
            (Arc::from("r"), None),
        ];
        let got = join_outcomes(answered(OK), answers, Some(metrics.clone())).await;
        assert_eq!(got.ok(), OK);
        assert_eq!(
            action_count(&metrics, "ok"),
            2,
            "the accepted one and the QoS 0 one"
        );
        assert_eq!(action_count(&metrics, "failed"), 1, "the refused one");

        let answers = vec![
            (Arc::from("r"), Some(answered(WITHHELD))),
            (Arc::from("r"), None),
        ];
        let got = join_outcomes(answered(REFUSED), answers, Some(metrics.clone())).await;
        assert_eq!(got.ok(), REFUSED);
        assert_eq!(
            action_count(&metrics, "failed"),
            3,
            "both skipped behind the refusal"
        );
    }
}
