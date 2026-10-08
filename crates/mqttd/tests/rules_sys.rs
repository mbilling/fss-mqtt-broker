//! Watching the running rules on `$SYS` (ADR 0084): the per-rule statistics, driven
//! through the library — the rules a connection evaluates, the task main spawns, the hub
//! command it publishes with.
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
use mqtt_rules::RuleSet;
use mqtt_storage::MemorySessionStore;
use mqttd::hub::{Hub, HubCommand};
use mqttd::ingress::{IngressCredit, OverloadMode};
use mqttd::reload::LastReload;
use mqttd::rules::{ConnRules, PublishFacts, Publisher, Rules, RulesObserve};
use mqttd::rules_sys::run_stats;
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout, Instant};
use tokio_util::sync::CancellationToken;

/// The rules under watch, and everything a test reads them through.
struct Watched {
    rules: Rules,
    observe: Arc<RulesObserve>,
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
    let observe = RulesObserve::new();
    observe.apply(config, None);
    let rules =
        Rules::new(rx, Arc::from("n1"), Some(metrics.clone())).with_observe(observe.clone());
    Watched {
        rules,
        observe,
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
/// count for a refused or unrouted derived message (`delivery`). Its text is on `$SYS`
/// only while the trace is on, and a reload that removes or redefines the rule drops it.
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
            "no error text on $SYS with the trace off: {doc}"
        );
    }
    assert_eq!(
        w.observe.last_error("sqlerr").unwrap().message,
        "int(): cannot convert 'abc' to an integer"
    );

    w.observe.apply(&settings(2, true, 20), None);
    let (_, per_rule) = next_tick(&mut sys, 3).await;
    assert!(
        per_rule["sqlerr"]["last_error"]["message"]
            .as_str()
            .unwrap()
            .contains("'abc'"),
        "{}",
        per_rule["sqlerr"]
    );
    assert_eq!(
        per_rule["actfail"]["last_error"]["message"],
        "republish topic is reserved for the broker: $SYS/brokers/n1/rules"
    );
    assert_eq!(
        per_rule["fate"]["last_error"]["message"],
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

/// ADR 0084: the statistics reach a real subscriber through the real hub — `QoS` 0, not
/// retained, JSON — and never run a rule: the broker's own `$SYS` publishes are not
/// evaluated, even by a rule whose `FROM` names `$SYS` (it loads with a warning).
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
