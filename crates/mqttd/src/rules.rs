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
//! - **`QoS`.** For an inbound `QoS` 1/2 publish, every `QoS` ≥ 1 message its rules
//!   produce gets its own acknowledgement gate, and the publisher's PUBACK/PUBREC waits
//!   for the original **and** all of them ([`join_outcomes`]): "acked means owned"
//!   extends to what the rules produced. Inbound `QoS` 2 dedup means a rule fires
//!   exactly once per `QoS` 2 message. A `QoS` 0 publish has no acknowledgement, so
//!   nothing it produces is gated.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use mqtt_codec::QoS;
use mqtt_core::{AppProperties, ClientId};
use mqtt_observability::metrics::Metrics;
use mqtt_rules::{ClientInfo, Effect, EventInput, Outcome, PublishInput, Republish, Rule, RuleSet};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, warn};

use crate::hub::{HubCommand, PublishOutcome};

/// The live rule set, swapped by a reload (ADR 0032 validate-before-swap).
pub type RulesWatch = watch::Receiver<Arc<RuleSet>>;

/// The rule engine as the broker uses it: the live rules, this node's id (the `node`
/// field) and the metrics they report into.
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
        Outcome::ActionOk => (None, Some("ok")),
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

    /// Turn effects into republishes, logging the console ones.
    fn collect(effects: Vec<(Arc<str>, Effect)>) -> Vec<Republish> {
        effects
            .into_iter()
            .filter_map(|(rule, e)| match e {
                Effect::Republish(r) => Some(r),
                Effect::Console(json) => {
                    info!(rule = %rule, output = %json, "rule console action");
                    None
                }
            })
            .collect()
    }

    /// Evaluate a client publish. Returns the messages its rules republish, in rule
    /// then action order. Empty — and nearly free — when no rule selects messages.
    #[must_use]
    pub fn on_publish(&self, f: &PublishFacts<'_>) -> Vec<Republish> {
        let set = self.current();
        if !set.has_message_rules() {
            return Vec::new();
        }
        let mut input = PublishInput::new(&f.client.0, f.topic, f.payload, qos_num(f.qos), f.app);
        input.username = f.publisher.username.as_deref();
        input.peer = f.publisher.peer;
        input.retain = f.retain;
        input.dup = f.dup;
        input.message_expiry = f.message_expiry;
        input.node = &self.node;
        let mut effects = Vec::new();
        let metrics = self.metrics.as_deref();
        set.on_publish(&input, &mut |r, o| report(metrics, r, o), &mut effects);
        Self::collect(effects)
    }

    /// Whether any rule selects `kind` (checked before building the event).
    #[must_use]
    pub fn wants(&self, kind: mqtt_rules::EventKind) -> bool {
        self.current().wants_event(kind)
    }

    /// The `ClientInfo` a client/session event is built from.
    #[must_use]
    pub fn client_info<'a>(&'a self, client: &'a ClientId, p: &'a Publisher) -> ClientInfo<'a> {
        ClientInfo {
            clientid: &client.0,
            username: p.username.as_deref(),
            peer: p.peer,
            sockname: None,
            node: &self.node,
        }
    }

    /// Evaluate a client/session event and publish what its rules produce. Events
    /// gate nothing — there is no publisher acknowledgement to hold — so the
    /// republishes go to the hub ungated, as a Will does.
    pub fn fire_event(&self, input: &EventInput, hub: &mpsc::UnboundedSender<HubCommand>) {
        let set = self.current();
        let mut effects = Vec::new();
        let metrics = self.metrics.as_deref();
        set.on_event(input, &mut |r, o| report(metrics, r, o), &mut effects);
        for r in Self::collect(effects) {
            let _ = hub.send(derived_command(r, None, None));
        }
    }
}

/// The hub command for one rule-produced message.
///
/// `v5: false` is deliberate: the v5 answer to a retained-quota overflow is to refuse
/// the publish, which for a derived message would refuse the *original's*
/// acknowledgement over a rule's retain flag; the v3.1.1 answer — deliver it live,
/// retain nothing — is the right one for a message no client sent.
#[must_use]
pub fn derived_command(
    r: Republish,
    done: Option<oneshot::Sender<PublishOutcome>>,
    credit: Option<crate::ingress::IngressPermit>,
) -> HubCommand {
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
        credit,
    }
}

/// Send `derived` behind an original publish whose command is already sent, and
/// return the receiver the publisher's acknowledgement should wait on.
///
/// `original` is the original's ack-gate receiver (`None` for `QoS` 0). Each derived
/// message at `QoS` ≥ 1 behind a gated original gets its own gate; `QoS` 0 ones
/// promise nothing and are not waited for. `credit` — the original's ingress permit —
/// rides the LAST command, so it is held until the whole batch has been dispatched
/// (ADR 0082: the hub's data lane is FIFO, so the last command dispatches last).
#[must_use]
pub fn send_derived(
    hub: &mpsc::UnboundedSender<HubCommand>,
    original: Option<oneshot::Receiver<PublishOutcome>>,
    derived: Vec<Republish>,
    mut credit: Option<crate::ingress::IngressPermit>,
) -> Option<oneshot::Receiver<PublishOutcome>> {
    let gated = original.is_some();
    let n = derived.len();
    let mut answers = Vec::new();
    for (i, r) in derived.into_iter().enumerate() {
        let done = if gated && r.qos > 0 {
            let (tx, rx) = oneshot::channel();
            answers.push(rx);
            Some(tx)
        } else {
            None
        };
        let credit = if i + 1 == n { credit.take() } else { None };
        let _ = hub.send(derived_command(r, done, credit));
    }
    original.map(|rx| join_outcomes(rx, answers))
}

/// One acknowledgement for a publish and the gated messages its rules produced.
///
/// - all accepted → `Accepted`;
/// - all **refused** → the original's refusal (nothing was stored anywhere, so the
///   publisher may be told so);
/// - anything else — a withheld answer anywhere, or some stored and some refused — is
///   **withheld**: the receiver closes, the connection sends no ack, and the publisher
///   retries. A refusal claims "nothing was stored", which a half-stored batch cannot
///   honestly say (the rule `refuse_pending` already enforces per message, issue #238).
#[must_use]
pub fn join_outcomes(
    original: oneshot::Receiver<PublishOutcome>,
    derived: Vec<oneshot::Receiver<PublishOutcome>>,
) -> oneshot::Receiver<PublishOutcome> {
    if derived.is_empty() {
        return original;
    }
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        if let Some(outcome) = combine(original, derived).await {
            let _ = tx.send(outcome);
        }
        // `None`: dropping `tx` withholds.
    });
    rx
}

async fn combine(
    original: oneshot::Receiver<PublishOutcome>,
    derived: Vec<oneshot::Receiver<PublishOutcome>>,
) -> Option<PublishOutcome> {
    let first = original.await.ok()?;
    let mut all_accepted = first == PublishOutcome::Accepted;
    let mut all_refused = !all_accepted;
    for d in derived {
        match d.await.ok()? {
            PublishOutcome::Accepted => all_refused = false,
            PublishOutcome::Refused(_) => all_accepted = false,
        }
    }
    if all_accepted {
        Some(PublishOutcome::Accepted)
    } else if all_refused {
        Some(first)
    } else {
        None
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
        join_outcomes(answered(first), rest.iter().map(|o| answered(*o)).collect())
            .await
            .ok()
    }

    const OK: Option<PublishOutcome> = Some(PublishOutcome::Accepted);
    const REFUSED: Option<PublishOutcome> = Some(PublishOutcome::Refused(PublishRefusal::Brownout));
    const WITHHELD: Option<PublishOutcome> = None;

    #[tokio::test]
    async fn the_publisher_is_acked_only_when_everything_was_accepted() {
        assert_eq!(
            joined(OK, &[]).await,
            OK,
            "no derived: the original's own answer"
        );
        assert_eq!(joined(OK, &[OK, OK]).await, OK);
        assert_eq!(
            joined(REFUSED, &[REFUSED]).await,
            REFUSED,
            "stored nowhere: sayable"
        );
        // Half stored: a refusal would claim nothing was stored — withhold instead.
        assert_eq!(joined(OK, &[REFUSED]).await, WITHHELD);
        assert_eq!(joined(REFUSED, &[OK]).await, WITHHELD);
        assert_eq!(joined(OK, &[OK, WITHHELD]).await, WITHHELD);
        assert_eq!(joined(WITHHELD, &[OK]).await, WITHHELD);
    }
}
