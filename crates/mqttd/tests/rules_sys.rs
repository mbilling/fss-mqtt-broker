//! Watching the running rules on `$SYS` (ADR 0084): the per-rule statistics and the rule
//! trace, driven through the library — the rules a connection evaluates, the tasks main
//! spawns, the hub command they publish with.
//!
//! The statistics tests run on tokio's paused clock: a tick is exactly its interval after
//! the last one, so rates are exact and "an interval change applies at once" is a
//! measurement, not a race. The one test that needs a real hub and a real subscriber runs
//! on the real clock with bounded waits.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use mqtt_cluster::NodeId;
use mqtt_codec::QoS;
use mqtt_core::{AppProperties, ClientId};
use mqtt_observability::metrics::Metrics;
use mqtt_rules::{ClientInfo, EventInput, RuleSet};
use mqtt_storage::MemorySessionStore;
use mqttd::hub::{Hub, HubCommand};
use mqttd::ingress::{IngressCredit, OverloadMode};
use mqttd::reload::LastReload;
use mqttd::rules::{
    ConnRules, PublishFacts, Publisher, Rules, RulesObserve, TraceRecord, TRACE_QUEUE_BYTES,
};
use mqttd::rules_sys::{record_json, run_stats, run_trace};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout, Instant};
use tokio_util::sync::CancellationToken;

/// The rules under watch, and everything a test reads them through.
struct Watched {
    rules: Rules,
    observe: Arc<RulesObserve>,
    trace_rx: Option<mpsc::Receiver<TraceRecord>>,
    metrics: Arc<Metrics>,
    rules_tx: watch::Sender<Arc<RuleSet>>,
}

fn rule_set(text: &str) -> Arc<RuleSet> {
    Arc::new(
        RuleSet::parse(text)
            .unwrap_or_else(|e| panic!("test rules must load: {e}"))
            .rules,
    )
}

/// `[rules]` settings: statistics every `interval` seconds, the trace `trace` at `rate`.
fn settings(interval: u64, trace: bool, rate: u32) -> mqtt_config::Rules {
    mqtt_config::Rules {
        sys_interval_secs: interval,
        trace,
        trace_rate: rate,
        ..mqtt_config::Rules::default()
    }
}

fn watched(text: &str, config: &mqtt_config::Rules) -> Watched {
    let metrics = Arc::new(Metrics::new("test"));
    let (rules_tx, rx) = watch::channel(rule_set(text));
    let (observe, trace_rx) = RulesObserve::new();
    observe.apply(config, None);
    let rules =
        Rules::new(rx, Arc::from("n1"), Some(metrics.clone())).with_observe(observe.clone());
    Watched {
        rules,
        observe,
        trace_rx: Some(trace_rx),
        metrics,
        rules_tx,
    }
}

/// A pool no test fills.
fn plenty() -> Arc<IngressCredit> {
    Arc::new(IngressCredit::new(64 << 20, 1 << 20, OverloadMode::Pause))
}

/// Run a client publish of `payload` on `topic` through `conn`'s rules.
fn publish(conn: &ConnRules, topic: &str, payload: &[u8]) {
    let client = ClientId("c1".into());
    let publisher = Publisher {
        username: Some("u1".into()),
        peer: None,
    };
    let _ = conn.on_publish(&PublishFacts {
        client: &client,
        publisher: &publisher,
        topic,
        payload: &Bytes::copy_from_slice(payload),
        qos: QoS::AtLeastOnce,
        retain: false,
        dup: false,
        app: &AppProperties::default(),
        message_expiry: None,
    });
}

/// Spawn the statistics task over `w`, publishing into the returned receiver.
fn spawn_stats(
    w: &Watched,
    ingress: Arc<IngressCredit>,
    last_reload: Arc<LastReload>,
) -> (mpsc::UnboundedReceiver<HubCommand>, CancellationToken) {
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    let stop = CancellationToken::new();
    tokio::spawn(run_stats(
        w.rules.clone(),
        Some(w.metrics.clone()),
        hub_tx,
        ingress,
        last_reload,
        std::time::SystemTime::now(),
        stop.clone(),
    ));
    (hub_rx, stop)
}

/// The next `$SYS` message, within an hour of (paused) time.
async fn next_sys(rx: &mut mpsc::UnboundedReceiver<HubCommand>) -> (String, Value) {
    let cmd = timeout(Duration::from_secs(3600), rx.recv())
        .await
        .expect("a $SYS message within an hour");
    match cmd {
        Some(HubCommand::SysPublish { topic, payload, .. }) => {
            (topic, serde_json::from_slice(&payload).unwrap())
        }
        other => panic!("expected a SysPublish, got {other:?}"),
    }
}

/// One whole tick: the summary, then each rule's message by id.
async fn next_tick(
    rx: &mut mpsc::UnboundedReceiver<HubCommand>,
    rules: usize,
) -> (Value, BTreeMap<String, Value>) {
    let (topic, summary) = next_sys(rx).await;
    assert_eq!(
        topic, "$SYS/brokers/n1/rules",
        "a tick opens with its summary"
    );
    let mut per_rule = BTreeMap::new();
    for _ in 0..rules {
        let (topic, doc) = next_sys(rx).await;
        let id = doc["rule"].as_str().unwrap().to_string();
        assert_eq!(topic, format!("$SYS/brokers/n1/rules/{id}"));
        per_rule.insert(id, doc);
    }
    (summary, per_rule)
}

const THREE_RULES: &str = r#"
[rules.hot]
sql = 'SELECT payload.v AS v FROM "t/#" WHERE payload.v > 2'
actions = [{ function = "console" }]

[rules.cold]
sql = 'SELECT * FROM "never/#"'
actions = [{ function = "console" }]

[rules.off]
sql = 'SELECT * FROM "t/#"'
actions = [{ function = "console" }]
enable = false
"#;

/// ADR 0084 D4: every rule, enabled or not, has its message each tick, with its counts
/// since the broker started, its rates over the measured time since the last tick, and
/// when it last matched. Reading a never-matched rule's counts creates no series.
#[tokio::test(start_paused = true)]
async fn the_statistics_list_every_rule_with_counts_rates_and_last_activity() {
    let w = watched(THREE_RULES, &settings(2, false, 20));
    let (mut sys, _stop) = spawn_stats(&w, plenty(), Arc::new(LastReload::default()));

    let (summary, per_rule) = next_tick(&mut sys, 3).await;
    assert_eq!(summary["node"], "n1");
    assert_eq!(summary["interval_secs"], 2);
    assert_eq!(
        (summary["rules"].as_u64(), summary["enabled"].as_u64()),
        (Some(3), Some(2))
    );
    assert_eq!(summary["digest"], w.rules.current().digest());
    assert_eq!(summary["trace"], false);
    assert_eq!(summary["trace_rate"], 20);
    assert_eq!(summary["trace_dropped"], 0);
    assert_eq!(summary["stats_dropped"], 0);
    assert_eq!(summary["reload"], Value::Null);
    for key in ["at", "started_at"] {
        let t = summary[key].as_str().unwrap();
        assert!(
            t.ends_with('Z') && t.len() == 24,
            "{key}: RFC 3339 UTC, ms: {t}"
        );
    }
    assert_eq!(
        per_rule["off"]["enabled"], false,
        "a disabled rule is listed"
    );
    assert_eq!(per_rule["hot"]["enabled"], true);
    assert_eq!(per_rule["hot"]["def"].as_str().unwrap().len(), 16);
    assert_eq!(per_rule["hot"]["counts"]["matched"], 0);
    assert_eq!(per_rule["hot"]["last_active_at"], Value::Null);
    assert_eq!(per_rule["hot"]["last_error"], Value::Null);
    for doc in per_rule.values() {
        assert!(
            doc.get("sql").is_none() && doc.get("description").is_none(),
            "{doc}"
        );
    }

    let conn = w.rules.for_connection();
    for v in [1, 5, 7] {
        publish(&conn, "t/1", format!("{{\"v\":{v}}}").as_bytes());
    }
    let before = Instant::now();
    let (_, per_rule) = next_tick(&mut sys, 3).await;
    assert_eq!(
        Instant::now() - before,
        Duration::from_secs(2),
        "one interval later"
    );
    let hot = &per_rule["hot"];
    assert_eq!(
        hot["counts"],
        serde_json::json!({"matched": 3, "passed": 2, "no_result": 1, "failed": 0,
                           "actions_ok": 2, "actions_failed": 0})
    );
    assert_eq!(
        hot["rates"],
        serde_json::json!({"matched": 1.5, "passed": 1.0, "no_result": 0.5, "failed": 0.0,
                           "actions_ok": 1.0, "actions_failed": 0.0}),
        "growth over the 2 s measured between ticks"
    );
    let active = hot["last_active_at"].as_str().unwrap().to_string();
    assert_eq!(per_rule["cold"]["last_active_at"], Value::Null);
    assert_eq!(
        per_rule["off"]["counts"]["matched"], 0,
        "a disabled rule never runs"
    );

    // Quiet: no rates, and the last activity stays where it was.
    let (_, per_rule) = next_tick(&mut sys, 3).await;
    assert_eq!(per_rule["hot"]["rates"]["matched"], 0.0);
    assert_eq!(per_rule["hot"]["counts"]["matched"], 3, "cumulative");
    assert_eq!(per_rule["hot"]["last_active_at"], active.as_str());

    let rendered = w.metrics.render();
    assert!(
        rendered.contains("rule=\"hot\""),
        "the matched rule has series"
    );
    assert!(
        !rendered.contains("rule=\"cold\"") && !rendered.contains("rule=\"off\""),
        "reading a never-matched rule's counts created a series:\n{rendered}"
    );
}

/// [`THREE_RULES`] without `hot`.
const HOT_REMOVED: &str = r#"
[rules.cold]
sql = 'SELECT * FROM "never/#"'
actions = [{ function = "console" }]

[rules.off]
sql = 'SELECT * FROM "t/#"'
actions = [{ function = "console" }]
enable = false
"#;

/// ADR 0084 D4: `last_active_at` is the tick at which the statistics saw a rule's
/// evaluations grow. What ran before they were on, or while they were off, was not seen
/// to run. A reload that removes a rule and one that puts it back keep its last activity
/// with its counts, which are kept by rule id: the rule put back is active again only
/// once it runs.
#[tokio::test(start_paused = true)]
async fn last_active_is_kept_across_a_reload_and_set_only_when_a_rule_runs() {
    let w = watched(THREE_RULES, &settings(0, false, 20));
    let (mut sys, _stop) = spawn_stats(&w, plenty(), Arc::new(LastReload::default()));
    let conn = w.rules.for_connection();
    publish(&conn, "t/1", br#"{"v":5}"#);
    w.observe.apply(&settings(2, false, 20), None);
    let (_, per_rule) = next_tick(&mut sys, 3).await;
    assert_eq!(per_rule["hot"]["counts"]["matched"], 1);
    assert_eq!(
        per_rule["hot"]["last_active_at"],
        Value::Null,
        "it ran before the statistics were on"
    );

    publish(&conn, "t/1", br#"{"v":5}"#);
    let (summary, per_rule) = next_tick(&mut sys, 3).await;
    let ran = per_rule["hot"]["last_active_at"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(ran, summary["at"].as_str().unwrap(), "seen at this tick");

    w.rules_tx.send(rule_set(HOT_REMOVED)).unwrap();
    next_tick(&mut sys, 2).await;
    w.rules_tx.send(rule_set(THREE_RULES)).unwrap();
    let (_, per_rule) = next_tick(&mut sys, 3).await;
    assert_eq!(per_rule["hot"]["counts"]["matched"], 2, "kept by rule id");
    assert_eq!(
        per_rule["hot"]["last_active_at"],
        ran.as_str(),
        "put back, and not run since"
    );

    publish(&conn, "t/1", br#"{"v":5}"#);
    let (summary, per_rule) = next_tick(&mut sys, 3).await;
    assert_eq!(per_rule["hot"]["last_active_at"], summary["at"]);
    assert_ne!(per_rule["hot"]["last_active_at"], ran.as_str());
    let active = summary["at"].as_str().unwrap().to_string();

    // Off, a run nobody sees, and on again.
    w.observe.apply(&settings(0, false, 20), None);
    assert!(
        timeout(Duration::from_secs(60), sys.recv()).await.is_err(),
        "off: no tick"
    );
    publish(&conn, "t/1", br#"{"v":5}"#);
    w.observe.apply(&settings(2, false, 20), None);
    let (_, per_rule) = next_tick(&mut sys, 3).await;
    assert_eq!(per_rule["hot"]["counts"]["matched"], 4);
    assert_eq!(
        per_rule["hot"]["last_active_at"],
        active.as_str(),
        "the run while off was not seen"
    );
    assert_eq!(per_rule["cold"]["last_active_at"], Value::Null);
}

/// ADR 0084: the settings are live. A shorter interval applies at once — the next tick
/// is the new interval after the last, not after the old one — off stops the ticks, and
/// on again ticks at once when that is already due.
#[tokio::test(start_paused = true)]
async fn an_interval_change_applies_at_once() {
    let w = watched(THREE_RULES, &settings(3600, false, 20));
    let (mut sys, _stop) = spawn_stats(&w, plenty(), Arc::new(LastReload::default()));
    next_tick(&mut sys, 3).await;
    let first = Instant::now();

    w.observe.apply(&settings(2, false, 20), None);
    next_tick(&mut sys, 3).await;
    assert_eq!(Instant::now() - first, Duration::from_secs(2), "not 3600 s");

    w.observe.apply(&settings(0, false, 20), None);
    assert!(
        timeout(Duration::from_secs(7200), sys.recv())
            .await
            .is_err(),
        "off: no tick in two hours"
    );
    let off_for = Instant::now();
    w.observe.apply(&settings(5, false, 20), None);
    next_tick(&mut sys, 3).await;
    assert_eq!(Instant::now(), off_for, "overdue, so at once");
}

/// The next `$SYS` message's topic and Message Expiry Interval, within an hour of
/// (paused) time.
async fn next_expiry(rx: &mut mpsc::UnboundedReceiver<HubCommand>) -> (String, u32) {
    match timeout(Duration::from_secs(3600), rx.recv()).await {
        Ok(Some(HubCommand::SysPublish {
            topic,
            message_expiry,
            ..
        })) => (topic, message_expiry),
        other => panic!("expected a SysPublish, got {other:?}"),
    }
}

/// ADR 0084: every `$SYS` message carries a Message Expiry Interval, so a copy that is
/// queued anyway (by a peer that predates live-only delivery) expires: two statistics
/// intervals, at least 10 seconds, and 10 seconds for a trace record.
#[tokio::test(start_paused = true)]
async fn every_sys_message_carries_a_message_expiry() {
    let w = watched(THREE_RULES, &settings(30, false, 20));
    let (mut sys, _stop) = spawn_stats(&w, plenty(), Arc::new(LastReload::default()));
    for (interval, expiry) in [(30, 60), (3, 10), (600, 1200)] {
        w.observe.apply(&settings(interval, false, 20), None);
        // A summary and one message per rule.
        for _ in 0..4 {
            let (topic, got) = next_expiry(&mut sys).await;
            assert_eq!(got, expiry, "{topic} every {interval} s");
        }
    }

    let mut w = watched(TRACED, &settings(0, true, 20));
    publish(&w.rules.for_connection(), "t/1", br#"{"v":"x"}"#);
    let (hub_tx, mut hub_rx) = mpsc::unbounded_channel();
    tokio::spawn(run_trace(
        w.trace_rx.take().unwrap(),
        w.rules.clone(),
        hub_tx,
        plenty(),
        CancellationToken::new(),
    ));
    let (topic, expiry) = next_expiry(&mut hub_rx).await;
    assert_eq!(topic, "$SYS/brokers/n1/trace/rules/pub");
    assert_eq!(expiry, 10);
}

/// ADR 0084 / ADR 0082: each statistics message takes node-pool credit first. A pool too
/// short for the tick skips the rest of it and counts it — here every rule message, since
/// the summary still holds the pool's only byte until the hub (this test) drops it.
#[tokio::test(start_paused = true)]
async fn a_tick_the_pool_cannot_carry_is_skipped_and_counted() {
    let w = watched(THREE_RULES, &settings(2, false, 20));
    let tiny = Arc::new(IngressCredit::new(1, 1, OverloadMode::Pause));
    let (mut sys, _stop) = spawn_stats(&w, tiny.clone(), Arc::new(LastReload::default()));

    let (topic, first) = next_sys(&mut sys).await;
    assert_eq!(topic, "$SYS/brokers/n1/rules");
    assert_eq!(first["stats_dropped"], 0);
    let (topic, second) = next_sys(&mut sys).await;
    assert_eq!(
        topic, "$SYS/brokers/n1/rules",
        "no rule message fitted in the pool"
    );
    assert_eq!(second["stats_dropped"], 1);
    assert_eq!(w.observe.stats_dropped(), 2, "the second tick too");
    assert_eq!(
        tiny.in_use(),
        0,
        "every message handed over returned its credit"
    );
}

/// ADR 0084: a rule's last error is kept from the failure the log reports (`sql` or
/// `action`, a `$SYS` republish refused included) or synthesized from the failed-action
/// count for a refused or unrouted derived message (`delivery`). `$SYS` shows its time and
/// kind, the admin API its text too, and a reload that removes or redefines the rule
/// drops it.
#[tokio::test(start_paused = true)]
async fn last_errors_by_kind_and_a_synthesized_delivery_entry() {
    let text = r#"
[rules.sqlerr]
sql = 'SELECT int(payload.v) AS w FROM "e/#"'
actions = []

[rules.actfail]
sql = 'SELECT payload.t AS t FROM "e/#"'
actions = [{ function = "republish", args = { topic = "${t}" } }]

[rules.fate]
sql = 'SELECT * FROM "e/#"'
actions = [{ function = "republish", args = { topic = "out/x" } }]
"#;
    let w = watched(text, &settings(2, false, 20));
    let (mut sys, _stop) = spawn_stats(&w, plenty(), Arc::new(LastReload::default()));
    next_tick(&mut sys, 3).await;

    let conn = w.rules.for_connection();
    publish(&conn, "e/1", br#"{"v":"abc","t":"$SYS/brokers/n1/rules"}"#);
    // The hub counts a derived message it refuses or cannot route, with no error text.
    for _ in 0..3 {
        w.metrics.rule_action("fate", "failed");
    }
    let (_, per_rule) = next_tick(&mut sys, 3).await;
    assert_eq!(per_rule["sqlerr"]["last_error"]["kind"], "sql");
    assert_eq!(per_rule["actfail"]["last_error"]["kind"], "action");
    assert_eq!(
        per_rule["actfail"]["counts"]["actions_failed"], 1,
        "the $SYS republish failed"
    );
    assert_eq!(per_rule["fate"]["last_error"]["kind"], "delivery");
    for doc in per_rule.values() {
        assert!(
            doc["last_error"].get("message").is_none(),
            "no error text on $SYS: {doc}"
        );
    }
    // The texts the admin API shows an operator.
    let message = |id: &str| w.observe.last_error(id).unwrap().message;
    assert_eq!(
        message("sqlerr"),
        "int(): cannot convert 'abc' to an integer"
    );
    assert_eq!(
        message("actfail"),
        "republish topic is reserved for the broker: $SYS/brokers/n1/rules"
    );
    assert_eq!(
        message("fate"),
        "3 derived message(s) failed (refused or not routed)"
    );

    // A reload: `sqlerr` redefined, `fate` gone. Their errors were about rules that no
    // longer run.
    w.rules_tx
        .send(rule_set(
            r#"
[rules.sqlerr]
sql = 'SELECT int(payload.v) AS w FROM "e/#" WHERE payload.v = 1'
actions = []

[rules.actfail]
sql = 'SELECT payload.t AS t FROM "e/#"'
actions = [{ function = "republish", args = { topic = "${t}" } }]
"#,
        ))
        .unwrap();
    let (_, per_rule) = next_tick(&mut sys, 2).await;
    assert_eq!(per_rule["sqlerr"]["last_error"], Value::Null);
    assert_eq!(
        per_rule["actfail"]["last_error"]["kind"], "action",
        "unchanged: kept"
    );
    assert!(w.observe.last_error("fate").is_none());
}

/// ADR 0084: a rule's last error on `$SYS` is its time and kind, never its text, with
/// the trace off or on: the text can quote a payload value, and a reader granted the
/// statistics need not be one granted the trace. The text is kept, for the trace and
/// the admin API.
#[tokio::test(start_paused = true)]
async fn sys_statistics_never_carry_error_text() {
    let w = watched(
        r#"
[rules.sqlerr]
sql = 'SELECT int(payload.v) AS w FROM "e/#"'
actions = []
"#,
        &settings(2, false, 20),
    );
    let (mut sys, _stop) = spawn_stats(&w, plenty(), Arc::new(LastReload::default()));
    next_tick(&mut sys, 1).await;
    publish(&w.rules.for_connection(), "e/1", br#"{"v":"s3cret-value"}"#);
    assert!(
        w.observe
            .last_error("sqlerr")
            .unwrap()
            .message
            .contains("'s3cret-value'"),
        "the kept text quotes the payload"
    );
    for trace in [false, true] {
        w.observe.apply(&settings(2, trace, 20), None);
        let (summary, per_rule) = next_tick(&mut sys, 1).await;
        assert_eq!(summary["trace"], trace);
        let error = &per_rule["sqlerr"]["last_error"];
        let keys: Vec<&String> = error.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["at", "kind"], "trace {trace}: {error}");
        assert_eq!(error["kind"], "sql");
        for doc in std::iter::once(&summary).chain(per_rule.values()) {
            let text = doc.to_string();
            assert!(!text.contains("s3cret"), "trace {trace}: {text}");
        }
    }
}

/// ADR 0084: a reload rejected for a config file whose broken line holds a secret puts
/// only the failing part on `$SYS` — never the text, which quotes that line — and the
/// rules' SQL (which can hold a pseudonym salt) is never there either, trace on or off.
#[tokio::test(start_paused = true)]
async fn a_secret_in_a_config_error_or_in_a_rule_never_reaches_sys() {
    let dir = common::TempDir::new();
    let path = dir.path().join("mqttd.toml");
    std::fs::write(&path, "[cluster.swim]\nkey = \"s3cret-gossip-key\n").unwrap();
    let w = watched(
        r#"
[rules.pseudo]
sql = '''SELECT sha256(concat('pepper-salt-77', clientid)) AS id FROM "t/#"'''
actions = [{ function = "console" }]
"#,
        &settings(2, true, 20),
    );
    let allow = || -> mqttd::reload::BuildResult {
        Ok((
            Arc::new(mqtt_auth::AllowAll) as Arc<dyn mqtt_auth::Authorizer>,
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }) as Arc<dyn mqtt_auth::Authenticator>,
        ))
    };
    let (mut reloader, _handles) = mqttd::reload::Reloader::new(
        allow().unwrap(),
        Arc::new(mqtt_observability::AuditLog::new()),
        allow,
    );
    reloader.attach_config_source(mqttd::reload::ConfigSource {
        live: Arc::new(RwLock::new(mqtt_config::Config::default())),
        path: Some(path),
        precheck: Box::new(|_| Ok(())),
        apply: Box::new(|_, _| Vec::new()),
    });
    let last = Arc::new(LastReload::default());
    reloader.attach_last_reload(last.clone());
    assert!(!reloader.reload("watch"));
    let error = last.get().unwrap().error.unwrap();
    assert!(
        error.contains("s3cret"),
        "the error does quote the line: {error}"
    );

    let (mut sys, _stop) = spawn_stats(&w, plenty(), last);
    publish(&w.rules.for_connection(), "t/1", b"{}");
    let (summary, per_rule) = next_tick(&mut sys, 1).await;
    assert_eq!(
        summary["reload"],
        serde_json::json!({
            "at": summary["reload"]["at"],
            "trigger": "watch",
            "applied": false,
            "error_kind": "config",
            "repeats": 0,
        })
    );
    for doc in std::iter::once(&summary).chain(per_rule.values()) {
        let text = doc.to_string();
        assert!(!text.contains("s3cret"), "a secret on $SYS: {text}");
        assert!(!text.contains("pepper-salt"), "rule SQL on $SYS: {text}");
    }
}

const TRACED: &str = r#"
[rules.pub]
sql = 'SELECT payload.v AS v FROM "t/#"'
actions = [
  { function = "republish", args = { topic = "out/${v}", payload = "${v}" } },
  { function = "console" },
  { function = "republish", args = { topic = "${v}" } },
]

[rules.raw]
sql = 'SELECT payload FROM "bin/#"'
actions = []

[rules.event]
sql = 'SELECT clientid FROM "$events/client/connected"'
actions = []
"#;

fn drain(rx: &mut mpsc::Receiver<TraceRecord>) -> Vec<TraceRecord> {
    let mut out = Vec::new();
    while let Ok(r) = rx.try_recv() {
        out.push(r);
    }
    out
}

/// ADR 0084 D5: with the trace off an evaluation queues nothing.
#[tokio::test]
async fn the_trace_off_records_nothing() {
    let mut w = watched(TRACED, &settings(0, false, 20));
    let conn = w.rules.for_connection();
    publish(&conn, "t/1", br#"{"v":"x"}"#);
    assert!(drain(w.trace_rx.as_mut().unwrap()).is_empty());
    assert_eq!(w.observe.trace_dropped(), 0);
}

/// Fire each kind of trigger at [`TRACED`]'s rules: three publishes (one past 1 KiB, one
/// not UTF-8), a Will and a client's connect. Returns the records, as published.
fn fire_every_trigger(w: &mut Watched) -> Vec<Value> {
    let conn = w.rules.for_connection();
    publish(&conn, "t/1", br#"{"v":"$SYS/x"}"#);
    let long = format!("{{\"v\":\"{}\"}}", "a".repeat(3000));
    publish(&conn, "t/2", long.as_bytes());
    publish(&conn, "bin/1", b"\xff\x00\xfe");
    w.rules.on_will(
        &PublishFacts {
            client: &ClientId("dying".into()),
            publisher: &Publisher::default(),
            topic: "t/will",
            payload: &Bytes::from_static(br#"{"v":"bye"}"#),
            qos: QoS::AtMostOnce,
            retain: true,
            dup: false,
            app: &AppProperties::default(),
            message_expiry: None,
        },
        |_| {},
    );
    let info = ClientInfo {
        clientid: "ev1",
        username: Some("eve"),
        peer: None,
        sockname: None,
        node: "n1",
    };
    conn.fire_event(
        &EventInput::client_connected(&info, 5, 30, true, 0, 0),
        &mpsc::unbounded_channel().0,
    );
    drain(w.trace_rx.as_mut().unwrap())
        .iter()
        .map(|r| record_json("n1", r))
        .collect()
}

/// ADR 0084 D5: a trace record shows the trigger — a publish, a Will or an event, with
/// its client, username, topic, `QoS`, retain flag and up to 1 KiB of payload (as text, or
/// base64 when it is not) — the SQL's result, and what each action rendered, a failed
/// action as its index and error.
#[tokio::test]
async fn trace_records_show_the_trigger_and_what_the_rule_rendered() {
    let mut w = watched(TRACED, &settings(0, true, 20));
    let records = fire_every_trigger(&mut w);
    assert_eq!(records.len(), 5, "{records:#?}");

    let first = &records[0];
    assert_eq!(
        (first["node"].as_str(), first["rule"].as_str()),
        (Some("n1"), Some("pub"))
    );
    assert_eq!(first["result"], "passed");
    assert_eq!(first["error"], Value::Null);
    let trigger = &first["trigger"];
    assert_eq!(trigger["type"], "publish");
    assert_eq!(trigger["topic"], "t/1");
    assert_eq!(
        (trigger["qos"].as_u64(), trigger["retain"].as_bool()),
        (Some(1), Some(false))
    );
    assert_eq!(
        (trigger["clientid"].as_str(), trigger["username"].as_str()),
        (Some("c1"), Some("u1"))
    );
    assert_eq!(trigger["payload"], r#"{"v":"$SYS/x"}"#);
    assert_eq!(trigger["payload_encoding"], "utf8");
    assert_eq!(trigger["truncated"], false);
    let outputs = first["outputs"].as_array().unwrap();
    assert_eq!(outputs[0]["action"], "republish");
    assert_eq!(outputs[0]["topic"], "out/$SYS/x");
    assert_eq!(outputs[0]["payload"], "$SYS/x");
    assert_eq!(
        outputs[1],
        serde_json::json!({"action": "console", "output": {"v": "$SYS/x"}})
    );
    assert_eq!(
        outputs[2]["action_index"], 2,
        "the third action, refused at render"
    );
    assert_eq!(
        outputs[2]["error"],
        "republish topic is reserved for the broker: $SYS/x"
    );
    assert_eq!(first["outputs_omitted"], 0);

    let long = &records[1]["trigger"];
    assert_eq!(long["payload_bytes"], 3008);
    assert_eq!(long["truncated"], true);
    assert_eq!(
        long["payload"].as_str().unwrap().len(),
        1024,
        "1 KiB is copied"
    );
    assert_eq!(
        records[1]["outputs"][0]["truncated"], true,
        "an output's payload too"
    );

    let raw = &records[2]["trigger"];
    assert_eq!(
        (raw["payload"].as_str(), raw["payload_encoding"].as_str()),
        (Some("/wD+"), Some("base64"))
    );

    let will = &records[3]["trigger"];
    assert_eq!(
        (will["type"].as_str(), will["clientid"].as_str()),
        (Some("will"), Some("dying"))
    );
    assert_eq!(will["retain"], true);

    assert_eq!(records[4]["rule"], "event");
    assert_eq!(
        records[4]["trigger"],
        serde_json::json!({"type": "event", "event": "client.connected",
                           "clientid": "ev1", "username": "eve"})
    );
}

/// The `at` second of a record, for counting per second.
fn second_of(r: &TraceRecord) -> u64 {
    r.at.duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// ADR 0084 D5: at most `trace_rate` records per rule per second, and `no_result` ones on
/// a budget of their own, so a rule whose WHERE rarely passes is still traced when it does.
#[tokio::test]
async fn the_trace_is_rate_limited_per_rule_and_no_result_has_its_own_budget() {
    let mut w = watched(
        r#"
[rules.r]
sql = 'SELECT * FROM "t/#" WHERE payload.ok = true'
actions = []
"#,
        &settings(0, true, 3),
    );
    let conn = w.rules.for_connection();
    for _ in 0..20 {
        publish(&conn, "t/1", br#"{"ok":false}"#);
    }
    for _ in 0..20 {
        publish(&conn, "t/1", br#"{"ok":true}"#);
    }
    let records = drain(w.trace_rx.as_mut().unwrap());
    let mut per_second: BTreeMap<(u64, &str), usize> = BTreeMap::new();
    for r in &records {
        *per_second
            .entry((second_of(r), r.result.as_str()))
            .or_default() += 1;
    }
    assert!(per_second.values().all(|n| *n <= 3), "{per_second:?}");
    for result in ["passed", "no_result"] {
        assert!(
            records
                .iter()
                .filter(|r| r.result.as_str() == result)
                .count()
                >= 3,
            "{result} records were traced after 20 no_result ones: {per_second:?}"
        );
    }
    assert_eq!(
        w.observe.trace_dropped(),
        0,
        "a rate-limited evaluation is not a drop"
    );
}

/// ADR 0084 D5: the trace task publishes at most max(`trace_rate`, 200) records a second
/// for the node, each on its rule's trace topic, and counts the rest as dropped.
#[tokio::test(start_paused = true)]
async fn the_trace_task_holds_the_node_ceiling() {
    let mut w = watched(
        r#"
[rules.a]
sql = 'SELECT * FROM "t/#"'
actions = []

[rules.b]
sql = 'SELECT * FROM "t/#"'
actions = []

[rules.c]
sql = 'SELECT * FROM "t/#"'
actions = []
"#,
        &settings(0, true, 100),
    );
    let conn = w.rules.for_connection();
    for _ in 0..100 {
        publish(&conn, "t/1", b"{}");
    }
    assert_eq!(w.observe.trace_dropped(), 0, "all 300 queued");
    let (hub_tx, mut hub_rx) = mpsc::unbounded_channel();
    tokio::spawn(run_trace(
        w.trace_rx.take().unwrap(),
        w.rules.clone(),
        hub_tx,
        plenty(),
        CancellationToken::new(),
    ));
    let mut published = 0;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(500), hub_rx.recv()).await {
        let HubCommand::SysPublish { topic, payload, .. } = cmd else {
            panic!("expected a SysPublish");
        };
        let doc: Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            topic,
            format!(
                "$SYS/brokers/n1/trace/rules/{}",
                doc["rule"].as_str().unwrap()
            )
        );
        published += 1;
    }
    assert_eq!(published + w.observe.trace_dropped(), 300);
    assert_eq!(published, 200, "the node ceiling, in one (paused) second");
}

/// ADR 0084 D5: queued records are bounded in bytes as well as in number: records past
/// the byte budget are dropped and counted, long before the queue's 1024 slots fill.
#[tokio::test]
async fn the_trace_queue_is_bounded_in_bytes() {
    let actions = (0..16)
        .map(|i| format!("{{ function = \"republish\", args = {{ topic = \"o/{i}\", payload = \"${{payload}}\" }} }}"))
        .collect::<Vec<_>>()
        .join(",\n  ");
    let mut w = watched(
        &format!("[rules.big]\nsql = 'SELECT * FROM \"t/#\"'\nactions = [\n  {actions}\n]\n"),
        &settings(0, true, 1000),
    );
    let conn = w.rules.for_connection();
    let payload = vec![b'x'; 1024];
    for _ in 0..400 {
        publish(&conn, "t/1", &payload);
    }
    let records = drain(w.trace_rx.as_mut().unwrap());
    let bytes: usize = records.iter().map(TraceRecord::weight).sum();
    assert!(
        records.len() < 400 && records.len() < 1024,
        "{} queued",
        records.len()
    );
    assert!(bytes <= TRACE_QUEUE_BYTES, "{bytes} bytes queued");
    assert_eq!(records.len() as u64 + w.observe.trace_dropped(), 400);
    assert!(records[0].outputs.len() == 16, "16 outputs of 1 KiB each");
}

/// ADR 0084: the statistics reach a real subscriber through the real hub — `QoS` 0, not
/// retained, JSON — and never run a rule: the broker's own `$SYS` publishes are not
/// evaluated, even by a rule whose `FROM` names `$SYS`.
#[tokio::test]
async fn sys_publishes_reach_subscribers_and_never_run_rules() {
    let w = watched(
        r##"
[rules.sys]
sql = 'SELECT * FROM "$SYS/#"'
actions = [{ function = "console" }]

[rules.all]
sql = 'SELECT * FROM "#"'
actions = [{ function = "console" }]
"##,
        &settings(1, false, 20),
    );
    let (mut hub, hub_tx) =
        Hub::with_config(NodeId("n1".into()), Arc::new(MemorySessionStore::new()));
    hub.attach_metrics(w.metrics.clone());
    tokio::spawn(hub.run());
    hub_tx
        .send(HubCommand::AttachRules(w.rules.clone()))
        .unwrap();
    let policy = Arc::new(mqttd::conn::ConnPolicy {
        anonymous: None,
        auth: mqttd::conn::auth_handle(Arc::new(mqtt_auth::basic::BasicAuthenticator {
            allow_anonymous: true,
        })),
        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
        identity_source: mqtt_auth::mtls::IdentitySource::default(),
        audit: Arc::new(mqtt_observability::AuditLog::new()),
        proxy: None,
        node: None,
        store: None,
        connect_timeout: Duration::from_secs(10),
        enhanced: None,
        shutdown: None,
        metrics: Some(w.metrics.clone()),
        ingress: None,
        rules: Some(w.rules.clone()),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_tx = hub_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle_stream(
                stream,
                Some(peer),
                None,
                policy.clone(),
                accept_tx.clone(),
            ));
        }
    });
    let mut watcher = common::Client::connect_v5_ok(addr, "watcher").await;
    watcher
        .subscribe(1, "$SYS/brokers/+/rules/#", QoS::AtLeastOnce)
        .await;
    tokio::spawn(run_stats(
        w.rules.clone(),
        Some(w.metrics.clone()),
        hub_tx,
        plenty(),
        Arc::new(LastReload::default()),
        std::time::SystemTime::now(),
        CancellationToken::new(),
    ));
    // A summary and one message per rule, every second; two ticks' worth.
    for _ in 0..6 {
        let p = watcher.expect_publish().await;
        assert!(p.topic.starts_with("$SYS/brokers/n1/rules"), "{}", p.topic);
        assert_eq!((p.qos, p.retain), (QoS::AtMostOnce, false));
        assert!(
            p.properties.0.iter().any(
                |prop| matches!(prop, mqtt_codec::Property::ContentType(c) if c == "application/json")
            ),
            "{:?}",
            p.properties
        );
        assert!(
            p.properties
                .0
                .contains(&mqtt_codec::Property::MessageExpiryInterval(10)),
            "two intervals of 1 s, at least 10 s: {:?}",
            p.properties
        );
        let doc: Value = serde_json::from_slice(&p.payload).unwrap();
        assert_eq!(doc["node"], "n1");
    }
    for rule in ["sys", "all"] {
        assert_eq!(
            w.metrics.rule_counts(rule),
            mqtt_observability::metrics::RuleCounts::default(),
            "{rule} ran on the broker's own $SYS message"
        );
    }
    let rendered = w.metrics.render();
    assert!(
        rendered.contains("mqttd_publish_received_total{qos=\"0\"}"),
        "a $SYS publish counts as a received QoS 0 publish:\n{rendered}"
    );
}

/// ADR 0084 D5: a record lists at most 16 of what the rule rendered and counts the rest
/// (a `FOREACH` runs every action once per output).
#[tokio::test]
async fn a_trace_record_lists_sixteen_outputs_and_counts_the_rest() {
    let mut w = watched(
        r#"
[rules.many]
sql = 'FOREACH payload.items DO item AS i FROM "many/#"'
actions = [{ function = "console" }]
"#,
        &settings(0, true, 20),
    );
    let items = (0..20).map(|i| i.to_string()).collect::<Vec<_>>().join(",");
    publish(
        &w.rules.for_connection(),
        "many/1",
        format!("{{\"items\":[{items}]}}").as_bytes(),
    );
    let records = drain(w.trace_rx.as_mut().unwrap());
    assert_eq!(records.len(), 1);
    assert_eq!(
        (records[0].outputs.len(), records[0].outputs_omitted),
        (16, 4)
    );
}

/// ADR 0084 D5: what the operator turns off stays off — records queued while the trace
/// was on are not published once it is off.
#[tokio::test(start_paused = true)]
async fn records_queued_before_the_trace_turned_off_are_not_published() {
    let mut w = watched(TRACED, &settings(0, true, 20));
    publish(&w.rules.for_connection(), "t/1", br#"{"v":"x"}"#);
    w.observe.apply(&settings(0, false, 20), None);
    let (hub_tx, mut hub_rx) = mpsc::unbounded_channel();
    tokio::spawn(run_trace(
        w.trace_rx.take().unwrap(),
        w.rules.clone(),
        hub_tx,
        plenty(),
        CancellationToken::new(),
    ));
    assert!(
        timeout(Duration::from_secs(60), hub_rx.recv())
            .await
            .is_err(),
        "a record was published after the trace was turned off"
    );
    assert_eq!(
        w.observe.trace_dropped(),
        0,
        "a discarded record is not a drop"
    );
}
