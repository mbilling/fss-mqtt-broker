//! The admin API's rules endpoints (ADR 0084 D6) over real mTLS: the running rules as a
//! viewer and as an operator read them, the rules file itself, a check and a dry run of
//! rules text, and the writes — the whole file, or one rule — with every refusal they
//! answer and what each leaves on disk.
//!
//! Each test runs one in-process admin listener over a rules file in a directory of its
//! own, wired as the broker wires it: the reloader rebuilds the rules from the file the live
//! config names, and the rule engine is watched with the trace on, so a dry run that
//! leaked into it would show. Certificates are minted per test: `viewer`, `operator` (an
//! operator not listed as a rules writer) and `writer` (an operator who is).

#[path = "common/skip.rs"]
mod skip;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use bytes::Bytes;
use mqtt_cluster::NodeId;
use mqtt_codec::QoS;
use mqtt_core::{AppProperties, ClientId};
use mqtt_observability::metrics::Metrics;
use mqtt_rules::RuleSet;
use mqtt_storage::MemorySessionStore;
use mqttd::admin::client::{self, Target};
use mqttd::admin::AdminState;
use mqttd::hub::{Hub, HubCommand};
use mqttd::ingress::{IngressCredit, OverloadMode};
use mqttd::reload::{LastReload, Reloader};
use mqttd::rules::{PublishFacts, Publisher, Rules, RulesObserve, TraceRecord};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};

/// An audit sink that keeps what it was given.
#[derive(Debug, Default)]
struct Recorded(Mutex<Vec<(String, Option<String>, String)>>);

impl mqtt_observability::AuditSink for Recorded {
    fn record(&self, kind: &str, subject: Option<&str>, detail: &str) {
        self.0
            .lock()
            .unwrap()
            .push((kind.into(), subject.map(String::from), detail.into()));
    }
}

impl Recorded {
    fn of_kind(&self, kind: &str) -> Vec<(Option<String>, String)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _, _)| k == kind)
            .map(|(_, s, d)| (s.clone(), d.clone()))
            .collect()
    }
}

/// The rules every test starts from: a file header, a section banner, a rule's own
/// comment, a `'''` statement, a disabled rule whose second action fails to render on
/// some payloads, a rule that fails on a non-numeric payload, and one with a load warning.
const RULES: &str = r#"# Rules for the admin API tests.
# The header stays where it is, whatever is deleted below it.

[rules.alpha]
description = "doubles v"
sql = 'SELECT payload.v * 2 AS v FROM "a/#"'
actions = [{ function = "republish", args = { topic = "out/a/${v}", payload = "${v}" } }]

# --- Section two ------------------------------------------------------------

# beta's own comment
[rules.beta]
sql = '''
SELECT payload.v AS v
FROM "b/#"
WHERE payload.v > 1'''
actions = [{ function = "republish", args = { topic = "out/b", payload = "${v}" } }]

[rules.fails]
sql = 'SELECT int(payload.v) AS n FROM "e/#"'
actions = [{ function = "console" }]

[rules.gamma]
enable = false
sql = 'SELECT payload.v AS v FROM "g/#"'
actions = [{ function = "console" }, { function = "republish", args = { topic = "out/g/${v}" } }]

[rules.linted]
enable = false
sql = 'SELECT * FROM "l/#" WHERE payload.kind = "on"'
actions = []
"#;

fn sha256_hex(text: &str) -> String {
    mqttd::reload::sha256_hex(text.as_bytes())
}

/// A CA and the directory its files go in.
struct Ca {
    dir: PathBuf,
    issuer: rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
    pem: PathBuf,
}

fn mint_ca(dir: &Path) -> Ca {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "rules admin CA");
    let issuer = rcgen::CertifiedIssuer::self_signed(params, key).unwrap();
    let pem = dir.join("ca.pem");
    std::fs::write(&pem, issuer.pem()).unwrap();
    Ca {
        dir: dir.to_path_buf(),
        issuer,
        pem,
    }
}

/// A leaf for `127.0.0.1` with Common Name `name`; `(cert.pem, key.pem)`.
fn mint_leaf(ca: &Ca, name: &str) -> (PathBuf, PathBuf) {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let cert = params.signed_by(&key, &ca.issuer).unwrap();
    let cert_path = ca.dir.join(format!("{name}.pem"));
    let key_path = ca.dir.join(format!("{name}.key"));
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

type Who = (PathBuf, PathBuf);

/// One admin listener over one rules file, and everything a test looks at.
struct Node {
    addr: String,
    ca: Ca,
    /// The rules directory (and the certificates, in a directory of their own).
    _dirs: (tempfile::TempDir, tempfile::TempDir),
    /// The rules file as the config names it.
    file: PathBuf,
    live: Arc<RwLock<mqtt_config::Config>>,
    audit: Arc<Recorded>,
    metrics: Arc<Metrics>,
    rules: Rules,
    observe: Arc<RulesObserve>,
    trace_rx: mpsc::Receiver<TraceRecord>,
    reloader: Arc<Reloader>,
    /// Set, every reload's policy build fails: a reload rejected for a reason other than
    /// the rules.
    break_policy: Arc<AtomicBool>,
    /// Set, the reload loads the rules from this file instead: what runs after a write is
    /// not what was written, as when another writer replaced the file in between.
    load_instead: Arc<Mutex<Option<PathBuf>>>,
    viewer: Who,
    operator: Who,
    writer: Who,
}

/// Where the configured rules file is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// `<dir>/rules.toml`.
    Plain,
    /// `<dir>/rules.toml` is a symlink to `<dir>/real/rules.toml`.
    Symlinked,
}

impl Node {
    async fn start(text: &str) -> Node {
        Self::start_as(text, Layout::Plain).await
    }

    #[allow(clippy::too_many_lines)] // the broker's wiring, one handle at a time
    async fn start_as(text: &str, layout: Layout) -> Node {
        let rules_dir = tempfile::tempdir().unwrap();
        let pki_dir = tempfile::tempdir().unwrap();
        let file = rules_dir.path().join("rules.toml");
        match layout {
            Layout::Plain => std::fs::write(&file, text).unwrap(),
            Layout::Symlinked => {
                let real = rules_dir.path().join("real");
                std::fs::create_dir(&real).unwrap();
                std::fs::write(real.join("rules.toml"), text).unwrap();
                #[cfg(unix)]
                std::os::unix::fs::symlink(real.join("rules.toml"), &file).unwrap();
            }
        }
        let mut config = mqtt_config::Config::default();
        config.rules.file = Some(file.display().to_string());
        config.rules.admin_writers = vec!["CN=writer".into()];
        config.admin.viewers = vec!["CN=viewer".into()];
        config.admin.operators = vec!["CN=operator".into(), "CN=writer".into()];
        let live = Arc::new(RwLock::new(config));

        let audit = Arc::new(Recorded::default());
        let metrics = Arc::new(Metrics::new("test"));
        let break_policy = Arc::new(AtomicBool::new(false));
        let policy = || {
            (
                Arc::new(mqtt_auth::AllowAll) as Arc<dyn mqtt_auth::Authorizer>,
                Arc::new(mqtt_auth::basic::BasicAuthenticator {
                    allow_anonymous: true,
                }) as Arc<dyn mqtt_auth::Authenticator>,
            )
        };
        let broken = break_policy.clone();
        let (mut reloader, _handles) =
            Reloader::with_metrics(policy(), audit.clone(), Some(metrics.clone()), move || {
                if broken.load(Ordering::Relaxed) {
                    Err("cannot read MQTTD_ACL_FILE (/etc/mqttd/acl.toml): gone".to_string())
                } else {
                    Ok(policy())
                }
            });
        // As the broker does it: what the rules may call, and the one store they keep
        // their state in (ADR 0085), made once and given to every load.
        let (functions, rule_state) =
            mqttd::rules::functions_and_state_from_config(&live.read().unwrap().rules);
        let initial = Arc::new(RuleSet::parse_with(text, &functions).unwrap().rules);
        let (rules_tx, rules_rx) = watch::channel(initial);
        let load_instead = Arc::new(Mutex::new(None::<PathBuf>));
        reloader.attach_rules(rules_tx, {
            let (live, instead) = (live.clone(), load_instead.clone());
            let functions = functions.clone();
            move || {
                let path = match instead.lock().unwrap().clone() {
                    Some(other) => Some(other.display().to_string()),
                    None => live.read().unwrap().rules.file.clone(),
                };
                mqttd::rules::load_with(path.as_deref(), &functions).map(Arc::new)
            }
        });
        let last_reload = Arc::new(LastReload::default());
        reloader.attach_last_reload(last_reload.clone());
        let reloader = Arc::new(reloader);

        let (observe, trace_rx) = RulesObserve::new();
        observe.apply(
            &mqtt_config::Rules {
                sys_interval_secs: 1,
                trace: true,
                ..mqtt_config::Rules::default()
            },
            None,
        );
        let rules = Rules::new(rules_rx, Arc::from("rules-node"), Some(metrics.clone()))
            .with_observe(observe.clone())
            .with_functions(functions)
            .with_state(rule_state);

        let ca = mint_ca(pki_dir.path());
        let (server_cert, server_key) = mint_leaf(&ca, "server");
        let acceptor =
            mqtt_net::tls::admin_acceptor(&server_cert, &server_key, &[ca.pem.as_path()]).unwrap();
        let (hub, hub_tx) = Hub::with_config(
            NodeId("rules-node".into()),
            Arc::new(MemorySessionStore::new()),
        );
        tokio::spawn(hub.run());
        let health = mqttd::health::HealthState::new(hub_tx, None, None, 1);
        let state = AdminState::new("rules-node".into(), health, live.clone(), audit.clone())
            .with_reload(mqttd::admin::config::ReloadAccess {
                reloader: reloader.clone(),
                stamp: Arc::new(mqttd::reload::ConfigStamp::default()),
            })
            .with_rules(mqttd::admin::rules::RulesAccess::new(
                rules.clone(),
                last_reload,
            ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(mqttd::admin::serve(listener, acceptor, state));
        let viewer = mint_leaf(&ca, "viewer");
        let operator = mint_leaf(&ca, "operator");
        let writer = mint_leaf(&ca, "writer");
        Node {
            addr,
            ca,
            _dirs: (rules_dir, pki_dir),
            file,
            live,
            audit,
            metrics,
            rules,
            observe,
            trace_rx,
            reloader,
            break_policy,
            load_instead,
            viewer,
            operator,
            writer,
        }
    }

    fn target(&self, who: &Who) -> Target {
        Target {
            addr: self.addr.clone(),
            server_name: "127.0.0.1".into(),
            connector: mqtt_net::tls::client_connector(&self.ca.pem, &who.0, &who.1).unwrap(),
            timeout: Duration::from_secs(10),
        }
    }

    /// `method path` with `body` as `who`; the status and the parsed answer.
    async fn call(
        &self,
        who: &Who,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> (u16, Value) {
        let body = body.map(Value::to_string);
        let (status, text) = client::call(&self.target(who), method, path, body.as_deref())
            .await
            .unwrap();
        (status, serde_json::from_str(&text).unwrap())
    }

    /// What the rules file holds now.
    fn on_disk(&self) -> String {
        std::fs::read_to_string(&self.file).unwrap()
    }

    fn running(&self) -> String {
        self.rules.current().digest().to_string()
    }

    /// A client publish of `payload` on `topic`, evaluated by the running rules as a
    /// connection evaluates it.
    fn publish(&self, topic: &str, payload: &[u8]) {
        let _ = self.rules.for_connection().on_publish(&PublishFacts {
            client: &ClientId("c1".into()),
            publisher: &Publisher::default(),
            topic,
            payload: &Bytes::copy_from_slice(payload),
            qos: QoS::AtMostOnce,
            retain: false,

            app: &AppProperties::default(),
            message_expiry: None,
        });
    }

    /// The rules series `/metrics` would show for `rule`.
    fn series_of(&self, rule: &str) -> Vec<String> {
        self.metrics
            .render()
            .lines()
            .filter(|l| l.starts_with("mqttd_rule_") && l.contains(&format!("rule=\"{rule}\"")))
            .map(String::from)
            .collect()
    }
}

fn code(body: &Value) -> &str {
    body.pointer("/error/code")
        .and_then(Value::as_str)
        .unwrap_or("")
}

/// The rule `id` of a `GET /admin/v1/rules` answer.
fn rule<'a>(answer: &'a Value, id: &str) -> &'a Value {
    answer["rules"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == id)
        .unwrap_or_else(|| panic!("no rule {id} in {answer}"))
}

// ---------------------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------------------

/// `GET /admin/v1/rules`: an operator reads what the rules are and what they did — SQL,
/// actions, counts, last activity, the last error's text, the warnings, the last reload's
/// text. A viewer reads the same rules redacted: the error texts quote payload values and
/// the rules file, and the SQL and warnings can hold a secret such as a pseudonym salt, so
/// none of them reach a viewer.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one answer read twice, field by field
async fn the_running_rules_are_read_whole_by_an_operator_and_redacted_for_a_viewer() {
    let n = Node::start(RULES).await;
    // The statistics' first tick takes their baseline; the next one sees the activity
    // since: when each rule last ran.
    let (hub_tx, mut hub_rx) = mpsc::unbounded_channel();
    let stop = tokio_util::sync::CancellationToken::new();
    tokio::spawn(mqttd::rules_sys::run_stats(
        n.rules.clone(),
        Some(n.metrics.clone()),
        hub_tx,
        Arc::new(IngressCredit::new(64 << 20, 1 << 20, OverloadMode::Pause)),
        Arc::new(LastReload::default()),
        std::time::SystemTime::now(),
        stop.clone(),
    ));
    for traffic in [false, true] {
        if traffic {
            // Live traffic: alpha passes twice, fails fails once and keeps its error.
            n.publish("a/1", br#"{"v":2}"#);
            n.publish("a/2", br#"{"v":3}"#);
            n.publish("e/1", br#"{"v":"s3cret-value"}"#);
        }
        // The tick's summary and five rule messages, then the tick is done.
        for _ in 0..6 {
            let sent = tokio::time::timeout(Duration::from_secs(10), hub_rx.recv()).await;
            assert!(
                matches!(sent, Ok(Some(HubCommand::SysPublish { .. }))),
                "a statistics tick within 10 s"
            );
        }
    }
    stop.cancel();
    // A reload rejected over a rules file whose text the error quotes, then the file put
    // back as it was: what runs is the file on disk again.
    std::fs::write(&n.file, "[rules.x]\nsql = 'SELECT \"hush-salt-42\"\n").unwrap();
    assert!(!n.reloader.reload_with_outcome("signal").applied);
    std::fs::write(&n.file, RULES).unwrap();

    let (status, op) = n.call(&n.operator, "GET", "/admin/v1/rules", None).await;
    assert_eq!(status, 200, "{op}");
    assert_eq!(op["node"], "rules-node");
    assert_eq!(op["digest"], sha256_hex(RULES));
    assert_eq!(
        (&op["file_digest"], &op["in_sync"]),
        (&json!(sha256_hex(RULES)), &json!(true))
    );
    assert_eq!(op["writable"], false, "an operator who is not a writer");
    assert!(op.get("redacted").is_none(), "{op}");
    let ids: Vec<&str> = op["rules"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["alpha", "beta", "fails", "gamma", "linted"]);
    let alpha = rule(&op, "alpha");
    assert_eq!(alpha["enabled"], true);
    assert_eq!(alpha["description"], "doubles v");
    assert_eq!(
        (&alpha["from"], &alpha["events"]),
        (&json!(["a/#"]), &json!([]))
    );
    assert_eq!(alpha["actions"], 1);
    assert_eq!(alpha["def"].as_str().unwrap().len(), 16);
    // The evaluation time varies run to run: there is some, and the average is its share.
    let mut counts = alpha["counts"].clone();
    let eval_ns = counts["eval_ns"].as_u64().expect("eval_ns");
    assert!(eval_ns > 0, "{counts}");
    assert_eq!(
        counts["eval_us_avg"],
        json!(
            (f64::from(u32::try_from(eval_ns).expect("a few evaluations")) / 2.0).round() / 1000.0
        ),
        "the average over its 2 evaluations, in microseconds: {counts}"
    );
    let map = counts.as_object_mut().unwrap();
    map.remove("eval_ns");
    map.remove("eval_us_avg");
    assert_eq!(
        counts,
        json!({"matched": 2, "passed": 2, "no_result": 0, "failed": 0, "actions_ok": 0,
               "actions_failed": 0}),
        "a derived message's action is counted when the hub routes it; none was sent here"
    );
    assert!(
        alpha["last_active_at"].as_str().unwrap().ends_with('Z'),
        "{alpha}"
    );
    assert_eq!(
        rule(&op, "beta")["last_active_at"],
        Value::Null,
        "beta never ran"
    );
    assert_eq!(alpha["sql"], "SELECT payload.v * 2 AS v FROM \"a/#\"");
    assert_eq!(
        alpha["actions_spec"],
        json!([{"function": "republish", "args": {"topic": "out/a/${v}", "payload": "${v}"}}])
    );
    let fails = rule(&op, "fails");
    assert_eq!(fails["counts"]["failed"], 1);
    assert_eq!(fails["last_error"]["kind"], "sql");
    assert!(
        fails["last_error"]["message"]
            .as_str()
            .unwrap()
            .contains("s3cret-value"),
        "{fails}"
    );
    assert!(
        op["warnings"][0]
            .as_str()
            .unwrap()
            .contains("rule `linted`"),
        "{op}"
    );
    assert_eq!(op["reload"]["trigger"], "signal");
    assert_eq!(
        (&op["reload"]["applied"], &op["reload"]["error_kind"]),
        (&json!(false), &json!("rules"))
    );
    assert!(
        op["reload"]["error"]
            .as_str()
            .unwrap()
            .contains("hush-salt-42"),
        "{op}"
    );

    let (status, writer) = n.call(&n.writer, "GET", "/admin/v1/rules", None).await;
    assert_eq!(
        (status, &writer["writable"]),
        (200, &json!(true)),
        "{writer}"
    );

    let (status, viewer) = n.call(&n.viewer, "GET", "/admin/v1/rules", None).await;
    assert_eq!(status, 200, "{viewer}");
    assert_eq!(
        (&viewer["redacted"], &viewer["writable"]),
        (&json!(true), &json!(false))
    );
    assert_eq!(viewer["digest"], op["digest"]);
    assert!(viewer.get("warnings").is_none(), "{viewer}");
    for r in viewer["rules"].as_array().unwrap() {
        assert_eq!(r["redacted"], true, "{r}");
        assert!(
            r.get("sql").is_none() && r.get("actions_spec").is_none(),
            "{r}"
        );
    }
    let fails = rule(&viewer, "fails");
    assert_eq!(fails["counts"], rule(&op, "fails")["counts"]);
    assert_eq!(fails["last_error"]["kind"], "sql");
    assert!(fails["last_error"].get("message").is_none(), "{fails}");
    assert!(viewer["reload"].get("error").is_none(), "{viewer}");
    assert_eq!(viewer["reload"]["error_kind"], "rules");
    let text = viewer.to_string();
    for secret in [
        "s3cret-value",
        "hush-salt-42",
        "payload.v * 2",
        "\\\"on\\\"",
    ] {
        assert!(!text.contains(secret), "{secret} reached a viewer: {text}");
    }
}

/// A rule's last error is about the definition that failed: once the rule is edited, the
/// error about its earlier SQL is no longer shown as its own.
#[tokio::test]
async fn a_last_error_is_shown_only_for_the_definition_it_was_about() {
    let n = Node::start(RULES).await;
    n.publish("e/1", br#"{"v":"not a number"}"#);
    let (_, body) = n.call(&n.operator, "GET", "/admin/v1/rules", None).await;
    assert_eq!(rule(&body, "fails")["last_error"]["kind"], "sql", "{body}");
    let fixed = json!({"sql": "SELECT payload.v AS n FROM \"e/#\"",
                       "actions": [{"function": "console"}], "description": "", "enable": true});
    let (status, body) = n
        .call(&n.writer, "PUT", "/admin/v1/rule?id=fails", Some(&fixed))
        .await;
    assert_eq!((status, &body["applied"]), (200, &json!(true)), "{body}");
    let (_, body) = n.call(&n.operator, "GET", "/admin/v1/rules", None).await;
    assert_eq!(rule(&body, "fails")["last_error"], Value::Null, "{body}");
}

/// `GET /admin/v1/rules/source`: the file on disk, verbatim, for an operator only — with
/// its digest and the running one, which differ once the file is edited by hand and not
/// reloaded. No file configured, or none on disk, is said so.
#[tokio::test]
async fn the_rules_file_is_read_verbatim_by_an_operator_only() {
    let n = Node::start(RULES).await;
    let (status, body) = n
        .call(&n.viewer, "GET", "/admin/v1/rules/source", None)
        .await;
    assert_eq!((status, code(&body)), (403, "forbidden"), "{body}");

    let (status, body) = n
        .call(&n.operator, "GET", "/admin/v1/rules/source", None)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["source"], RULES);
    assert_eq!(body["file"], n.file.display().to_string());
    assert_eq!(
        (&body["digest"], &body["running_digest"]),
        (&json!(sha256_hex(RULES)), &json!(sha256_hex(RULES)))
    );
    assert_eq!(
        (&body["in_sync"], &body["bytes"]),
        (&json!(true), &json!(RULES.len()))
    );
    assert_eq!(body["node"], "rules-node");

    let edited = format!("{RULES}# edited by hand\n");
    std::fs::write(&n.file, &edited).unwrap();
    let (_, body) = n
        .call(&n.operator, "GET", "/admin/v1/rules/source", None)
        .await;
    assert_eq!(body["source"], edited);
    assert_eq!(
        (&body["digest"], &body["running_digest"]),
        (&json!(sha256_hex(&edited)), &json!(sha256_hex(RULES)))
    );
    assert_eq!(body["in_sync"], false);

    std::fs::remove_file(&n.file).unwrap();
    let (status, body) = n
        .call(&n.operator, "GET", "/admin/v1/rules/source", None)
        .await;
    assert_eq!(
        (status, code(&body)),
        (409, "rules-file-unreadable"),
        "{body}"
    );
    n.live.write().unwrap().rules.file = None;
    let (status, body) = n
        .call(&n.operator, "GET", "/admin/v1/rules/source", None)
        .await;
    assert_eq!((status, code(&body)), (409, "rules-file-unset"), "{body}");
}

// ---------------------------------------------------------------------------------------
// Check and test: nothing written, nothing touched
// ---------------------------------------------------------------------------------------

/// `POST /admin/v1/rules/check`: a whole file, or one rule put into the file on disk,
/// either loads — with what it would hold — or is `422 rules-invalid` with where: a TOML
/// error at its line and column in the file, a SQL error at its line and column in the
/// rule's statement. A file on disk that does not parse cannot take one rule. Nothing is
/// written.
#[tokio::test]
#[allow(clippy::too_many_lines)] // every verdict a check gives
async fn check_says_whether_text_would_load_and_where_it_would_not() {
    let n = Node::start(RULES).await;
    let check = |body: Value| {
        let n = &n;
        async move {
            n.call(&n.operator, "POST", "/admin/v1/rules/check", Some(&body))
                .await
        }
    };
    let (status, body) = check(json!({"source": RULES})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (
            &body["valid"],
            &body["rules"],
            &body["enabled"],
            &body["digest"]
        ),
        (
            &json!(true),
            &json!(5),
            &json!(3),
            &json!(sha256_hex(RULES))
        )
    );
    assert!(
        body["warnings"][0]
            .as_str()
            .unwrap()
            .contains("rule `linted`"),
        "{body}"
    );

    let (status, body) =
        check(json!({"source": "[rules.a]\nsql = 'SELECT 1 FROM \"t\"'\nactions = [\n"})).await;
    assert_eq!((status, code(&body)), (422, "rules-invalid"), "{body}");
    assert_eq!(body["error"]["details"]["scope"], "toml", "{body}");
    assert_eq!(body["error"]["details"]["line"], 3, "{body}");
    assert!(
        body["error"]["details"]["column"].as_u64().is_some(),
        "{body}"
    );

    let (status, body) =
        check(json!({"source": "[rules.a]\nsql = '''\nSELECT *\nFROM \"t\" WHERE'''\n"})).await;
    assert_eq!((status, code(&body)), (422, "rules-invalid"), "{body}");
    assert_eq!(
        body["error"]["details"],
        json!({"scope": "sql", "rule": "a", "line": 2, "column": 15}),
        "the position is in the statement, not the file"
    );
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("rule `a`: "),
        "{body}"
    );

    let (status, body) = check(json!({"source": "[rules.a]\nsql = 'SELECT 1 FROM \"t\"'\nactions = [{ function = \"mail\" }]\n"})).await;
    assert_eq!((status, code(&body)), (422, "rules-invalid"), "{body}");
    assert_eq!(
        body["error"]["details"],
        json!({"scope": "rule", "rule": "a"}),
        "{body}"
    );

    // One rule, put into the file on disk: its left-out description and enable are the
    // on-disk rule's.
    let beta = json!({"id": "beta", "sql": "SELECT 1 AS one FROM \"b/#\"", "actions": []});
    let (status, body) = check(json!({"rule": beta})).await;
    assert_eq!(status, 200, "{body}");
    let expected = mqtt_rules::edit::put_rule(
        RULES,
        "beta",
        &serde_json::from_value(json!({"sql": "SELECT 1 AS one FROM \"b/#\"", "actions": [],
                                        "description": "", "enable": true}))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(body["digest"], sha256_hex(&expected));
    let (status, body) =
        check(json!({"rule": {"id": "beta", "sql": "SELECT FROM", "actions": []}})).await;
    assert_eq!((status, code(&body)), (422, "rules-invalid"), "{body}");
    assert_eq!(
        (
            &body["error"]["details"]["scope"],
            &body["error"]["details"]["rule"]
        ),
        (&json!("sql"), &json!("beta"))
    );

    let (status, body) = check(json!({"source": RULES, "rule": beta})).await;
    assert_eq!((status, code(&body)), (400, "bad-request"), "{body}");
    let (status, body) = check(json!({})).await;
    assert_eq!((status, code(&body)), (400, "bad-request"), "{body}");
    let (status, body) = check(json!({"source": RULES, "extra": 1})).await;
    assert_eq!((status, code(&body)), (400, "bad-request"), "{body}");

    assert_eq!(n.on_disk(), RULES, "a check writes nothing");
    assert_eq!(n.running(), sha256_hex(RULES), "and reloads nothing");
    std::fs::write(&n.file, "[rules\n").unwrap();
    let (status, body) = check(json!({"rule": beta})).await;
    assert_eq!((status, code(&body)), (409, "rules-file-invalid"), "{body}");

    let (status, body) = n
        .call(
            &n.viewer,
            "POST",
            "/admin/v1/rules/check",
            Some(&json!({"source": RULES})),
        )
        .await;
    assert_eq!((status, code(&body)), (403, "forbidden"), "{body}");
    assert!(n.audit.of_kind("rules.write").is_empty());
}

/// A rule whose output a 64-bit JSON reader would change: integers past 64 bits, a float
/// in jiffy's form, keys out of alphabetical order, and bytes that are not UTF-8.
const EXACT: &str = r#"
[rules.exact]
sql = '''SELECT payload.z AS z, payload.z + 1 AS next, payload.f AS f, payload.m AS m, sprintf('~c', 210) AS c FROM "t/#"'''
actions = [{ function = "console" }]
"#;

/// The payload [`EXACT`] reads, and its output as the rule renders it (and EMQX 6.3.1
/// does: `emqx_utils_json:encode/1` of the same map's values).
const EXACT_PAYLOAD: &str = r#"{"z":123456789012345678901234567890,"f":1e20,"m":{"b":1,"a":2}}"#;
const EXACT_OUTPUT: &str = "{\"z\":123456789012345678901234567890,\
     \"next\":123456789012345678901234567891,\"f\":1.0e20,\"m\":{\"b\":1,\"a\":2},\
     \"c\":\"\u{FFFD}\"}";

/// `POST /admin/v1/rules/test` answers a console output as the JSON text the rule
/// rendered: every digit of an integer past 64 bits, the float as written, the keys in
/// the statement's order. A 64-bit JSON reader on the way showed `1.2345678901234568e29`
/// and sorted the keys.
#[tokio::test]
async fn the_dry_run_answers_a_console_output_as_the_rule_rendered_it() {
    let n = Node::start(RULES).await;
    let body = json!({"source": EXACT, "topic": "t/1", "payload": EXACT_PAYLOAD}).to_string();
    let (status, text) = client::call(
        &n.target(&n.operator),
        "POST",
        "/admin/v1/rules/test",
        Some(&body),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "{text}");
    assert!(
        text.contains(&format!(
            "\"outputs\":[{{\"action\":\"console\",\"output\":{EXACT_OUTPUT}}}],"
        )),
        "{text}"
    );
    let doc: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        (
            &doc["node"],
            &doc["results"][0]["rule"],
            &doc["results"][0]["result"]
        ),
        (&json!("rules-node"), &json!("exact"), &json!("passed")),
        "{text}"
    );
    assert!(doc["results"][0].get("reason").is_none(), "{text}");
}

const STATEFUL: &str = r#"[state.s]
max_keys = 10

[rules.count]
state = "s"
sql = '''SELECT kv_incr('n') AS n, kv_store_put('p', 'dry') AS p, kv_store_get('seen', 'no') AS seen FROM "t/#"'''
actions = [{ function = "console" }]

[rules.read]
state = "s"
sql = '''SELECT kv_get('n') AS n FROM "t/#"'''
actions = [{ function = "console" }]
"#;

/// ADR 0085 D11: a dry run, and a check, never read or write the state the running
/// rules keep. `POST /admin/v1/rules/test` evaluates on a scratch store of its own, in
/// which the rules of one request see each other's writes, and which is gone with the
/// request.
#[tokio::test]
async fn a_dry_run_and_a_check_leave_the_running_rule_state_untouched() {
    use mqtt_rules::state::{NamespaceSettings, StateOp, StateStore};
    let n = Node::start(STATEFUL).await;
    let store = n
        .rules
        .state()
        .expect("rule state is on by default")
        .clone();
    // What the running store holds under `key` of namespace `ns`, read beside the rules.
    let held = |ns: &str, key: &[u8]| {
        let settings = NamespaceSettings::defaults(store.max_bytes());
        let handle = store.open(ns, &settings).unwrap();
        handle
            .call(&StateOp::Get { key })
            .unwrap()
            .value
            .map(|v| v.to_json().unwrap())
    };
    let snapshot = || {
        (
            store.namespaces(),
            store.bytes(),
            store.stats("s"),
            store.stats("kv_store"),
        )
    };
    // Live traffic: two messages counted, and EMQX's table written.
    n.publish("t/1", b"{}");
    n.publish("t/1", b"{}");
    assert_eq!(held("s", b"n").as_deref(), Some("2"));
    assert_eq!(store.namespaces(), ["kv_store", "s"]);
    let before = snapshot();
    // The dry run of the running set: an empty store, so the count starts at 1, and the
    // second rule of the same request reads what the first wrote there.
    for _ in 0..2 {
        let (status, body) = n
            .call(
                &n.operator,
                "POST",
                "/admin/v1/rules/test",
                Some(&json!({"topic": "t/1", "payload": "{}"})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            body["results"],
            json!([
                {"rule": "count", "enabled": true, "result": "passed", "error": null,
                 "outputs": [{"action": "console", "output": {"n": 1, "p": true, "seen": "no"}}]},
                {"rule": "read", "enabled": true, "result": "passed", "error": null,
                 "outputs": [{"action": "console", "output": {"n": 1}}]},
            ])
        );
    }
    // A dry run of other text: its namespaces are that text's, in the scratch store.
    let other = STATEFUL.replace("[state.s]", "[state.s]\n[state.fresh]");
    let (status, body) = n
        .call(
            &n.operator,
            "POST",
            "/admin/v1/rules/test",
            Some(&json!({"source": other, "only": "count", "topic": "t/1", "payload": "{}"})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["results"][0]["outputs"][0]["output"]["n"], 1, "{body}");
    // A check of text that would hold more state.
    let (status, body) = n
        .call(
            &n.operator,
            "POST",
            "/admin/v1/rules/check",
            Some(&json!({"source": other})),
        )
        .await;
    assert_eq!((status, &body["valid"]), (200, &json!(true)), "{body}");
    // Nothing of it reached the running store.
    assert_eq!(snapshot(), before);
    assert_eq!(held("s", b"n").as_deref(), Some("2"));
    n.publish("t/1", b"{}");
    assert_eq!(held("s", b"n").as_deref(), Some("3"));
}

/// `POST /admin/v1/rules/test`: what the running set, a `source` or one `rule` would do with
/// a message or an event — rendered outputs, a failed action's index, a SQL error, why a
/// rule would not run — while changing nothing: no Prometheus series, no last error, no
/// trace record, and the rule's once-per-interval failure report left for live traffic,
/// which still reports and traces as before.
#[tokio::test]
#[allow(clippy::too_many_lines)] // every kind of trigger and result, then what was not touched
async fn the_dry_run_shows_what_the_rules_would_do_and_changes_nothing() {
    let mut n = Node::start(RULES).await;
    let test = |body: Value| {
        let n = &n;
        async move {
            n.call(&n.operator, "POST", "/admin/v1/rules/test", Some(&body))
                .await
        }
    };
    // The running set: the enabled rules whose FROM selects the topic.
    let (status, body) = test(json!({"topic": "a/1", "payload": "{\"v\":2}"})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["node"], "rules-node");
    assert_eq!(
        body["results"],
        json!([{"rule": "alpha", "enabled": true, "result": "passed", "error": null,
                "outputs": [{"action": "republish", "topic": "out/a/4", "qos": 0,
                             "retain": false, "payload": "4", "payload_encoding": "utf8",
                             "payload_bytes": 1, "truncated": false}]}])
    );
    // One rule of it, on a topic it does not select: why not.
    let (_, body) = test(json!({"only": "beta", "topic": "a/1", "payload": "{}"})).await;
    assert_eq!(body["results"][0]["result"], "no_match");
    assert_eq!(
        body["results"][0]["reason"],
        "topic \"a/1\" matches none of the FROM filters (b/#)"
    );
    // A disabled rule runs when it is the one asked about, and says it is disabled; its
    // second action fails to render on this payload, and says which it was.
    let (_, body) =
        test(json!({"only": "gamma", "topic": "g/1", "payload": "{\"v\":\"x+y\"}"})).await;
    let gamma = &body["results"][0];
    assert_eq!(
        (&gamma["enabled"], &gamma["result"]),
        (&json!(false), &json!("passed")),
        "{body}"
    );
    assert_eq!(
        gamma["outputs"][0],
        json!({"action": "console", "output": {"v": "x+y"}})
    );
    assert_eq!(gamma["outputs"][1]["action_index"], 1, "{body}");
    assert!(
        gamma["outputs"][1]["error"]
            .as_str()
            .unwrap()
            .contains("out/g/x+y"),
        "{body}"
    );
    // Each failed action is named by its place in the rule's actions, whatever rendered
    // before it.
    let three = "[rules.three]\nsql = 'SELECT payload.v AS v FROM \"t/#\"'\nactions = [\
                 { function = \"republish\", args = { topic = \"x/${v}\" } }, \
                 { function = \"console\" }, \
                 { function = \"republish\", args = { topic = \"y/${v}\" } }]\n";
    let (_, body) =
        test(json!({"source": three, "topic": "t/1", "payload": "{\"v\":\"a+b\"}"})).await;
    let outputs = &body["results"][0]["outputs"];
    assert_eq!(
        (
            &outputs[0]["action_index"],
            &outputs[1]["action"],
            &outputs[2]["action_index"]
        ),
        (&json!(0), &json!("console"), &json!(2)),
        "{body}"
    );
    // A SQL failure.
    let (_, body) =
        test(json!({"only": "fails", "topic": "e/1", "payload": "{\"v\":\"nope\"}"})).await;
    assert_eq!(body["results"][0]["result"], "failed");
    assert!(
        body["results"][0]["error"]
            .as_str()
            .unwrap()
            .contains("nope"),
        "{body}"
    );
    // One rule as an editor holds it, put into the file on disk and run alone, enabled or
    // not; its left-out enable is the on-disk one.
    let (_, body) = test(
        json!({"rule": {"id": "gamma", "sql": "SELECT 7 AS seven FROM \"g/#\"",
                                          "actions": [{"function": "console"}]},
                                 "topic": "g/2", "payload": "{}"}),
    )
    .await;
    assert_eq!(
        body["results"],
        json!([{"rule": "gamma", "enabled": false, "result": "passed", "error": null,
                "outputs": [{"action": "console", "output": {"seven": 7}}]}])
    );
    // A whole source; an event; a payload given in base64, shown back as base64 when it
    // is not text.
    let source = "[rules.ev]\nsql = 'SELECT clientid FROM \"$events/client/connected\"'\nactions = [{ function = \"console\" }]\n\
                  [rules.bin]\nsql = 'SELECT payload FROM \"bin/#\"'\nactions = [{ function = \"republish\", args = { topic = \"raw\", payload = \"${payload}\" } }]\n";
    let (_, body) = test(
        json!({"source": source, "event": "client.connected", "topic": "",
                                 "payload": "", "clientid": "dev-7"}),
    )
    .await;
    assert_eq!(body["results"][0]["rule"], "ev", "{body}");
    assert_eq!(
        body["results"][0]["outputs"][0]["output"],
        json!({"clientid": "dev-7"})
    );
    let (_, body) = test(
        json!({"source": source, "topic": "bin/1", "payload": "AP8Q",
                                 "payload_encoding": "base64"}),
    )
    .await;
    let out = &body["results"][0]["outputs"][0];
    assert_eq!(
        (
            &out["payload"],
            &out["payload_encoding"],
            &out["payload_bytes"]
        ),
        (&json!("AP8Q"), &json!("base64"), &json!(3)),
        "{body}"
    );
    // Refusals: no client can publish into $SYS, a wildcard is not a topic, a rule the
    // set does not have, a body with both forms.
    let (status, body) = test(json!({"topic": "$SYS/brokers/x/rules", "payload": "{}"})).await;
    assert_eq!((status, code(&body)), (400, "topic-reserved"), "{body}");
    let (status, body) = test(json!({"topic": "a/+", "payload": "{}"})).await;
    assert_eq!((status, code(&body)), (400, "bad-request"), "{body}");
    let (status, body) = test(json!({"only": "nope", "topic": "a/1", "payload": "{}"})).await;
    assert_eq!((status, code(&body)), (404, "not-found"), "{body}");
    let (status, body) = test(json!({"rule": {"id": "x", "sql": "", "actions": []}, "only": "x", "topic": "a", "payload": ""})).await;
    assert_eq!((status, code(&body)), (400, "bad-request"), "{body}");

    // Nothing was touched.
    for id in ["alpha", "fails", "gamma"] {
        assert_eq!(
            n.series_of(id),
            Vec::<String>::new(),
            "a dry run created a series for {id}"
        );
    }
    assert!(
        n.observe.last_error("fails").is_none(),
        "a dry run kept an error"
    );
    assert!(
        matches!(n.trace_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "a dry run was traced"
    );
    assert_eq!(n.on_disk(), RULES);
    // Live traffic still has its failure report: the first live failure is stored (it is
    // stored only when it is the one reported) and traced.
    n.publish("e/1", br#"{"v":"live"}"#);
    let kept = n
        .observe
        .last_error("fails")
        .expect("the live failure is reported");
    assert!(kept.message.contains("live"), "{kept:?}");
    assert_eq!(n.trace_rx.try_recv().unwrap().rule.as_ref(), "fails");
    // One live failure: its evaluation, and the time it took (ADR 0084).
    let series = n.series_of("fails");
    assert_eq!(series.len(), 2, "{series:?}");
    assert!(
        series[0].starts_with("mqttd_rule_evaluations_total{rule=\"fails\",result=\"failed\"} 1")
            && series[1].starts_with("mqttd_rule_eval_seconds_total{rule=\"fails\"} "),
        "{series:?}"
    );
}

// ---------------------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------------------

/// `PUT /admin/v1/rules`: only a listed writer, only with `if_match` naming the file on
/// disk (or `*`), only text that loads. Then the file is replaced — its mode kept, the
/// old text kept as `.prev` — the ordinary reload runs, and the answer says what was
/// written and what runs. The write is audited.
#[tokio::test]
#[allow(clippy::too_many_lines)] // each refusal in order, then the write and its traces
async fn a_whole_file_write_is_a_writers_and_replaces_the_file_and_what_runs() {
    let n = Node::start(RULES).await;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&n.file, std::fs::Permissions::from_mode(0o640)).unwrap();
    }
    let new = "[rules.only]\nsql = 'SELECT * FROM \"o/#\"'\nactions = []\n";
    let put = |who: &Who, query: &str, source: &str| {
        let (n, who) = (&n, who.clone());
        let path = format!("/admin/v1/rules{query}");
        let body = json!({"source": source});
        async move { n.call(&who, "PUT", &path, Some(&body)).await }
    };
    let digest = sha256_hex(RULES);
    let (status, body) = put(&n.viewer, "?if_match=*", new).await;
    assert_eq!((status, code(&body)), (403, "forbidden"), "{body}");
    let (status, body) = put(&n.operator, "?if_match=*", new).await;
    assert_eq!((status, code(&body)), (403, "not-a-rules-writer"), "{body}");
    let (status, body) = put(&n.writer, "", new).await;
    assert_eq!(
        (status, code(&body)),
        (428, "precondition-required"),
        "{body}"
    );
    let (status, body) = put(&n.writer, "?if_match=0000", new).await;
    assert_eq!((status, code(&body)), (412, "digest-mismatch"), "{body}");
    assert_eq!(
        (&body["file_digest"], &body["running_digest"]),
        (&json!(digest), &json!(digest))
    );
    let (status, body) = put(
        &n.writer,
        &format!("?if_match={digest}"),
        "[rules.x]\nsql = 'SELECT'\n",
    )
    .await;
    assert_eq!((status, code(&body)), (422, "rules-invalid"), "{body}");
    assert_eq!(n.on_disk(), RULES, "nothing was written");
    assert!(!n.file.with_extension("toml.prev").exists());

    let (status, body) = put(&n.writer, &format!("?if_match={digest}"), new).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (&body["written"], &body["applied"]),
        (&json!(true), &json!(true)),
        "{body}"
    );
    assert_eq!(
        (&body["digest"], &body["running_digest"]),
        (&json!(sha256_hex(new)), &json!(sha256_hex(new)))
    );
    assert_eq!(
        (&body["rules"], &body["enabled"], &body["warnings"]),
        (&json!(1), &json!(1), &json!([]))
    );
    assert_eq!(
        (&body["reload"]["trigger"], &body["reload"]["applied"]),
        (&json!("admin-rules"), &json!(true)),
        "{body}"
    );
    assert_eq!(n.on_disk(), new);
    assert_eq!(n.running(), sha256_hex(new), "the new rules run");
    let prev = n.file.with_extension("toml.prev");
    assert_eq!(std::fs::read_to_string(&prev).unwrap(), RULES);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o7777;
        assert_eq!(
            (mode(&n.file), mode(&prev)),
            (0o640, 0o640),
            "the file's mode is kept"
        );
    }
    let leftovers: Vec<_> = std::fs::read_dir(n.file.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| Path::new(name).extension().is_some_and(|e| e == "tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    assert_eq!(
        n.audit.of_kind("rules.write"),
        [(
            Some("CN=writer".to_string()),
            format!(
                "op=put-file rule=- old={digest} new={} applied=true",
                sha256_hex(new)
            )
        )]
    );
    assert!(n
        .audit
        .of_kind("security.reload")
        .iter()
        .any(|(_, d)| d == "ok (trigger=admin-rules)"));

    // `*` replaces whatever is there; the same text again is no write and no reload.
    let (status, body) = put(&n.writer, "?if_match=*", new).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (&body["written"], &body["applied"], &body["reload"]),
        (&json!(false), &json!(true), &Value::Null),
        "{body}"
    );
    assert_eq!(
        n.audit.of_kind("rules.write").len(),
        1,
        "nothing written, nothing audited"
    );

    // No writers listed: writes are off for everyone.
    n.live.write().unwrap().rules.admin_writers.clear();
    let (status, body) = put(&n.writer, "?if_match=*", RULES).await;
    assert_eq!((status, code(&body)), (403, "rules-read-only"), "{body}");
    assert_eq!(n.on_disk(), new);
}

/// A rules file written where there was none is private to the broker (mode 0600): a
/// rules file can hold a secret, and nobody chose a wider mode for it.
#[cfg(unix)]
#[tokio::test]
async fn a_rules_file_written_where_there_was_none_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let n = Node::start(RULES).await;
    std::fs::remove_file(&n.file).unwrap();
    let new = "[rules.only]\nsql = 'SELECT * FROM \"o/#\"'\nactions = []\n";
    let (status, body) = n
        .call(
            &n.writer,
            "PUT",
            "/admin/v1/rules?if_match=*",
            Some(&json!({"source": new})),
        )
        .await;
    assert_eq!((status, &body["applied"]), (200, &json!(true)), "{body}");
    assert_eq!(n.on_disk(), new);
    let mode = std::fs::metadata(&n.file).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode, 0o600);
    assert!(!n.file.with_extension("toml.prev").exists());
}

/// `PUT /admin/v1/rule` and `DELETE /admin/v1/rule` change one rule and keep every other
/// byte of the file: an update writes only what differs, an insert is appended, a delete
/// takes the rule's header and keys and leaves the comments above it. Each applies at
/// once and is audited with the rule's id.
#[tokio::test]
#[allow(clippy::too_many_lines)] // update, insert, delete and their refusals on one file
async fn one_rule_is_edited_in_place_and_the_rest_of_the_file_kept_byte_for_byte() {
    let n = Node::start(RULES).await;
    let put = |id: &str, query: &str, body: Value| {
        let n = &n;
        let path = format!("/admin/v1/rule?id={id}{query}");
        async move { n.call(&n.writer, "PUT", &path, Some(&body)).await }
    };
    // Turn beta off: one line added after its last key, its ''' SQL untouched.
    let beta = json!({"sql": "SELECT payload.v AS v\nFROM \"b/#\"\nWHERE payload.v > 1",
                      "actions": [{"function": "republish", "args": {"topic": "out/b", "payload": "${v}"}}],
                      "description": "", "enable": false});
    let (status, body) = put("beta", "", beta).await;
    assert_eq!(status, 200, "{body}");
    let beta_actions = "actions = [{ function = \"republish\", args = { topic = \"out/b\", payload = \"${v}\" } }]\n";
    let expected = RULES.replacen(beta_actions, &format!("{beta_actions}enable = false\n"), 1);
    assert_eq!(n.on_disk(), expected);
    assert_eq!(
        (&body["applied"], &body["enabled"]),
        (&json!(true), &json!(2)),
        "{body}"
    );
    assert_eq!(n.running(), sha256_hex(&expected));
    assert!(
        !n.rules.current().get("beta").unwrap().enabled(),
        "beta is off, live"
    );

    // A new rule goes at the end; everything before it is the file as it was.
    let delta = json!({"sql": "SELECT payload.v AS v FROM \"d/#\"",
                       "actions": [{"function": "republish", "args": {"topic": "out/d"}}],
                       "description": "new", "enable": true});
    let (status, body) = put(
        "delta",
        &format!("&if_match={}", sha256_hex(&expected)),
        delta,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let with_delta = n.on_disk();
    assert!(with_delta.starts_with(&expected), "{with_delta}");
    let delta_rule = n.rules.current();
    let delta_rule = delta_rule.get("delta").expect("delta runs");
    assert_eq!(
        (delta_rule.description(), delta_rule.sql()),
        ("new", "SELECT payload.v AS v FROM \"d/#\"")
    );

    // Delete the first rule: the file header above it stays, and nothing else moves.
    let (status, body) = n
        .call(&n.writer, "DELETE", "/admin/v1/rule?id=alpha", None)
        .await;
    assert_eq!(status, 200, "{body}");
    let alpha = "[rules.alpha]\ndescription = \"doubles v\"\nsql = 'SELECT payload.v * 2 AS v FROM \"a/#\"'\nactions = [{ function = \"republish\", args = { topic = \"out/a/${v}\", payload = \"${v}\" } }]\n";
    assert_eq!(n.on_disk(), with_delta.replacen(alpha, "", 1));
    assert!(n.on_disk().starts_with("# Rules for the admin API tests."));
    assert!(
        n.rules.current().get("alpha").is_none(),
        "alpha is gone, live"
    );
    let writes = n.audit.of_kind("rules.write");
    let ops: Vec<&str> = writes
        .iter()
        .map(|(_, d)| d.split(" old=").next().unwrap())
        .collect();
    assert_eq!(
        ops,
        [
            "op=put-rule rule=beta",
            "op=put-rule rule=delta",
            "op=delete-rule rule=alpha"
        ]
    );

    // The refusals, each leaving the file as it is.
    let before = n.on_disk();
    let (status, body) = n
        .call(&n.writer, "DELETE", "/admin/v1/rule?id=alpha", None)
        .await;
    assert_eq!((status, code(&body)), (404, "not-found"), "{body}");
    let (status, body) = n
        .call(&n.writer, "DELETE", "/admin/v1/rule?id=9bad", None)
        .await;
    assert_eq!((status, code(&body)), (400, "bad-request"), "{body}");
    let (status, body) = n.call(&n.writer, "DELETE", "/admin/v1/rule", None).await;
    assert_eq!((status, code(&body)), (400, "bad-request"), "{body}");
    let (status, body) = n
        .call(
            &n.writer,
            "DELETE",
            "/admin/v1/rule?id=beta&if_match=0000",
            None,
        )
        .await;
    assert_eq!((status, code(&body)), (412, "digest-mismatch"), "{body}");
    let (status, body) = put(
        "beta",
        "",
        json!({"sql": "SELECT", "actions": [], "description": "", "enable": true}),
    )
    .await;
    assert_eq!((status, code(&body)), (422, "rules-invalid"), "{body}");
    assert_eq!(body["error"]["details"]["rule"], "beta", "{body}");
    let (status, body) = put(
        "beta",
        "",
        json!({"sql": "SELECT 1 FROM \"t\"", "actions": []}),
    )
    .await;
    assert_eq!(
        (status, code(&body)),
        (400, "bad-request"),
        "every field is required: {body}"
    );
    let (status, body) = n
        .call(&n.operator, "DELETE", "/admin/v1/rule?id=beta", None)
        .await;
    assert_eq!((status, code(&body)), (403, "not-a-rules-writer"), "{body}");
    assert_eq!(n.on_disk(), before);

    // A file a per-rule edit cannot keep intact, or that does not parse: refused.
    std::fs::write(&n.file, "rules.x.sql = 'SELECT * FROM \"t\"'\n").unwrap();
    let (status, body) = n
        .call(&n.writer, "DELETE", "/admin/v1/rule?id=x", None)
        .await;
    assert_eq!(
        (status, code(&body)),
        (409, "rules-layout-unsupported"),
        "{body}"
    );
    std::fs::write(&n.file, "[rules\n").unwrap();
    let (status, body) = n
        .call(&n.writer, "DELETE", "/admin/v1/rule?id=x", None)
        .await;
    assert_eq!((status, code(&body)), (409, "rules-file-invalid"), "{body}");
}

/// Two writes naming the same digest, sent at once: one is made, and the other is told
/// the file is no longer the one it names, with the digest the first wrote. The node
/// takes one write at a time from reading the file to its reload, so `if_match` is a
/// compare-and-swap and neither write is lost without a word. Twenty rounds, each on the
/// file as it started, so a race that only sometimes loses still shows.
#[tokio::test]
async fn two_writes_naming_the_same_digest_cannot_both_win() {
    let n = Node::start(RULES).await;
    let digest = sha256_hex(RULES);
    let rule = |topic: &str| {
        json!({"sql": format!("SELECT * FROM \"{topic}\""), "actions": [],
               "description": "", "enable": true})
    };
    let (one, two) = (rule("one/#"), rule("two/#"));
    let (path_one, path_two) = (
        format!("/admin/v1/rule?id=one&if_match={digest}"),
        format!("/admin/v1/rule?id=two&if_match={digest}"),
    );
    for round in 0..20 {
        std::fs::write(&n.file, RULES).unwrap();
        let (a, b) = tokio::join!(
            n.call(&n.writer, "PUT", &path_one, Some(&one)),
            n.call(&n.writer, "PUT", &path_two, Some(&two)),
        );
        let mut answers = [a, b];
        answers.sort_by_key(|(status, _)| *status);
        let [(made, winner), (refused, loser)] = answers;
        assert_eq!(
            (made, refused, code(&loser)),
            (200, 412, "digest-mismatch"),
            "round {round}: {winner} {loser}"
        );
        let on_disk = n.on_disk();
        let has = |id: &str| on_disk.contains(&format!("\n[rules.{id}]\n"));
        assert!(has("one") != has("two"), "round {round}: {on_disk}");
        assert_eq!(
            winner["digest"],
            json!(sha256_hex(&on_disk)),
            "round {round}"
        );
        assert_eq!(loser["file_digest"], winner["digest"], "round {round}");
    }
}

/// Every write is based on the file on disk, not on what runs: `if_match` names the file
/// as it was read, and an edit made by hand (and never reloaded) is kept by the next
/// per-rule write — and then runs.
#[tokio::test]
async fn if_match_and_every_edit_are_about_the_file_on_disk() {
    let n = Node::start(RULES).await;
    let by_hand = RULES.replace("doubles v", "doubles v, by hand");
    std::fs::write(&n.file, &by_hand).unwrap();
    let running = sha256_hex(RULES);
    let (status, body) = n
        .call(
            &n.writer,
            "DELETE",
            &format!("/admin/v1/rule?id=gamma&if_match={running}"),
            None,
        )
        .await;
    assert_eq!((status, code(&body)), (412, "digest-mismatch"), "{body}");
    assert_eq!(
        (&body["file_digest"], &body["running_digest"]),
        (&json!(sha256_hex(&by_hand)), &json!(running))
    );

    let (status, body) = n
        .call(
            &n.writer,
            "DELETE",
            &format!("/admin/v1/rule?id=gamma&if_match={}", sha256_hex(&by_hand)),
            None,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert!(
        n.on_disk().contains("doubles v, by hand"),
        "the hand edit is kept"
    );
    assert_eq!(
        n.rules.current().get("alpha").unwrap().description(),
        "doubles v, by hand"
    );

    // A per-rule write that changes nothing in a file that is not what runs: no write,
    // but the reload makes it run.
    std::fs::write(&n.file, RULES).unwrap();
    let gamma = json!({"sql": "SELECT payload.v AS v FROM \"g/#\"",
                       "actions": [{"function": "console"}, {"function": "republish", "args": {"topic": "out/g/${v}"}}],
                       "description": "", "enable": false});
    let (status, body) = n
        .call(&n.writer, "PUT", "/admin/v1/rule?id=gamma", Some(&gamma))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (
            &body["written"],
            &body["applied"],
            &body["reload"]["trigger"]
        ),
        (&json!(false), &json!(true), &json!("admin-rules")),
        "{body}"
    );
    assert_eq!(n.running(), sha256_hex(RULES));
}

/// A write whose reload is rejected for a reason that is not the rules — here, the ACL
/// build — has still written the file: the answer says so, with both digests, and the
/// running rules are the old ones.
#[tokio::test]
async fn a_rejected_reload_says_the_file_was_written_and_what_still_runs() {
    let n = Node::start(RULES).await;
    n.break_policy.store(true, Ordering::Relaxed);
    let new = "[rules.only]\nsql = 'SELECT * FROM \"o/#\"'\nactions = []\n";
    let (status, body) = n
        .call(
            &n.writer,
            "PUT",
            "/admin/v1/rules?if_match=*",
            Some(&json!({"source": new})),
        )
        .await;
    assert_eq!((status, code(&body)), (409, "reload-rejected"), "{body}");
    assert_eq!(
        (&body["written"], &body["digest"], &body["running_digest"]),
        (
            &json!(true),
            &json!(sha256_hex(new)),
            &json!(sha256_hex(RULES))
        )
    );
    assert_eq!(body["outcome"]["applied"], false);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("MQTTD_ACL_FILE"),
        "{body}"
    );
    assert_eq!(n.on_disk(), new);
    assert_eq!(n.running(), sha256_hex(RULES));
    assert_eq!(
        n.audit.of_kind("rules.write"),
        [(
            Some("CN=writer".to_string()),
            format!(
                "op=put-file rule=- old={} new={} applied=false",
                sha256_hex(RULES),
                sha256_hex(new)
            )
        )]
    );
    // The next reload that succeeds runs the file.
    n.break_policy.store(false, Ordering::Relaxed);
    let (status, body) = n
        .call(
            &n.writer,
            "PUT",
            "/admin/v1/rules?if_match=*",
            Some(&json!({"source": new})),
        )
        .await;
    assert_eq!(
        (status, &body["written"], &body["applied"]),
        (200, &json!(false), &json!(true)),
        "{body}"
    );
}

/// `applied` is whether what runs is what was written, not only whether the reload
/// applied: a reload that loaded other rules — another writer's, replacing the file in
/// between — applied, but not this write.
#[tokio::test]
async fn applied_means_the_written_rules_are_the_ones_running() {
    let n = Node::start(RULES).await;
    let other = n.file.with_file_name("other.toml");
    let other_text = "[rules.theirs]\nsql = 'SELECT * FROM \"t/#\"'\nactions = []\n";
    std::fs::write(&other, other_text).unwrap();
    *n.load_instead.lock().unwrap() = Some(other);
    let new = "[rules.only]\nsql = 'SELECT * FROM \"o/#\"'\nactions = []\n";
    let (status, body) = n
        .call(
            &n.writer,
            "PUT",
            "/admin/v1/rules?if_match=*",
            Some(&json!({"source": new})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["reload"]["applied"], true, "{body}");
    assert_eq!(
        (&body["written"], &body["applied"]),
        (&json!(true), &json!(false)),
        "{body}"
    );
    assert_eq!(
        (&body["digest"], &body["running_digest"]),
        (&json!(sha256_hex(new)), &json!(sha256_hex(other_text)))
    );
    assert!(n.audit.of_kind("rules.write")[0]
        .1
        .ends_with(" applied=false"));
}

/// A rules file that is a symlink has its target replaced: the link stays a link, as
/// config management left it.
#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_rules_file_has_its_target_replaced() {
    let n = Node::start_as(RULES, Layout::Symlinked).await;
    let new = "[rules.only]\nsql = 'SELECT * FROM \"o/#\"'\nactions = []\n";
    let (status, body) = n
        .call(
            &n.writer,
            "PUT",
            "/admin/v1/rules?if_match=*",
            Some(&json!({"source": new})),
        )
        .await;
    assert_eq!((status, &body["applied"]), (200, &json!(true)), "{body}");
    assert!(std::fs::symlink_metadata(&n.file)
        .unwrap()
        .file_type()
        .is_symlink());
    let real = n.file.parent().unwrap().join("real");
    assert_eq!(
        std::fs::read_to_string(real.join("rules.toml")).unwrap(),
        new
    );
    assert_eq!(
        std::fs::read_to_string(real.join("rules.toml.prev")).unwrap(),
        RULES
    );
}

/// A rules directory the broker may not write in: the write is refused with the OS error,
/// the file and the running rules are as they were, no temporary file is left, and the
/// rules read says the file is not writable.
#[cfg(unix)]
#[tokio::test]
async fn a_write_into_a_read_only_directory_is_refused_and_changes_nothing() {
    use std::os::unix::fs::PermissionsExt;
    let n = Node::start(RULES).await;
    let dir = n.file.parent().unwrap().to_path_buf();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let probe = dir.join("probe");
    if std::fs::write(&probe, "").is_ok() {
        let _ = std::fs::remove_file(&probe);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        crate::skip_locally_or_fail_in_ci!(
            "this user can create files in a mode-555 directory (running as root?), so a \
             directory the broker may not write in cannot be made here; run the suite as an \
             unprivileged user"
        );
    }
    let (status, body) = n
        .call(
            &n.writer,
            "PUT",
            "/admin/v1/rules?if_match=*",
            Some(&json!({"source": ""})),
        )
        .await;
    assert_eq!(
        (status, code(&body)),
        (409, "rules-file-unwritable"),
        "{body}"
    );
    assert!(
        body["os_error"]
            .as_str()
            .unwrap()
            .contains("ermission denied"),
        "{body}"
    );
    let (_, read) = n.call(&n.writer, "GET", "/admin/v1/rules", None).await;
    assert_eq!(read["writable"], false, "{read}");
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(n.on_disk(), RULES);
    assert_eq!(n.running(), sha256_hex(RULES));
    let names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, ["rules.toml"]);
    assert!(n.audit.of_kind("rules.write").is_empty());
}

/// Send a request head claiming a `length`-byte body, and no body: the answer is what the
/// node decides from the head alone.
async fn claim(n: &Node, who: &Who, method: &str, path: &str, length: usize) -> u16 {
    let connector = mqtt_net::tls::client_connector(&n.ca.pem, &who.0, &who.1).unwrap();
    let tcp = TcpStream::connect(&n.addr).await.unwrap();
    let mut tls = connector
        .connect(mqtt_net::tls::server_name("127.0.0.1").unwrap(), tcp)
        .await
        .unwrap();
    let head = format!("{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {length}\r\n\r\n");
    tls.write_all(head.as_bytes()).await.unwrap();
    tls.flush().await.unwrap();
    let mut raw = Vec::new();
    // The node answers and closes; a node waiting for the body would hit the bound.
    let read = tokio::time::timeout(Duration::from_secs(5), tls.read_to_end(&mut raw)).await;
    assert!(read.is_ok(), "no answer from the head alone");
    let text = String::from_utf8_lossy(&raw);
    text.split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// The body cap is decided from the caller's role before the body is read: a rules file
/// up to 1 MiB for a role that may call the route, the 64 KiB every other admin body has
/// for anyone else — a viewer, or a certificate in no list — who never makes the node
/// buffer more.
#[tokio::test]
async fn a_rules_body_is_read_only_from_a_role_that_may_send_one() {
    let n = Node::start(RULES).await;
    let stranger = mint_leaf(&n.ca, "stranger");
    let big = 200 * 1024;
    assert_eq!(
        claim(&n, &n.viewer, "POST", "/admin/v1/rules/check", big).await,
        413
    );
    assert_eq!(
        claim(&n, &stranger, "POST", "/admin/v1/rules/check", big).await,
        413
    );
    assert_eq!(
        claim(&n, &n.operator, "POST", "/admin/v1/rules/check", 2 << 20).await,
        413
    );
    assert_eq!(
        claim(&n, &n.writer, "PUT", "/admin/v1/rules", (1 << 20) + 1).await,
        413
    );
    assert_eq!(
        claim(&n, &n.operator, "POST", "/admin/v1/reload", big).await,
        413
    );
    // An operator's 200 KiB rules file is read, and checked.
    let padded = format!("{RULES}#{}\n", "x".repeat(big));
    let (status, body) = n
        .call(
            &n.operator,
            "POST",
            "/admin/v1/rules/check",
            Some(&json!({"source": padded})),
        )
        .await;
    assert_eq!((status, &body["valid"]), (200, &json!(true)), "{body}");
    let (status, body) = n
        .call(
            &n.writer,
            "PUT",
            "/admin/v1/rules?if_match=*",
            Some(&json!({"source": padded})),
        )
        .await;
    assert_eq!((status, &body["applied"]), (200, &json!(true)), "{body}");
    // A viewer's small body still reaches the role check.
    let (status, body) = n
        .call(
            &n.viewer,
            "POST",
            "/admin/v1/rules/check",
            Some(&json!({"source": ""})),
        )
        .await;
    assert_eq!((status, code(&body)), (403, "forbidden"), "{body}");
}

const DICTIONARY: &str = r#"[rules.a_put]
sql = '''SELECT proc_dict_put('n', payload.v) AS before, proc_dict_get('n') AS now FROM "t/#"'''
actions = [{ function = "console" }]

[rules.b_get]
sql = '''SELECT proc_dict_get('n') AS seen FROM "t/#"'''
actions = [{ function = "console" }]
"#;

/// ADR 0085 D11, D13: `POST /admin/v1/rules/test` evaluates EMQX's `proc_dict_*` on a
/// scratch dictionary of the request's own, in which its rules see each other's puts
/// (as the rules one connection's message matches do) and which is gone with the
/// request: never a connection's, and counted in no running store.
#[tokio::test]
async fn a_dry_run_works_on_a_scratch_dictionary_of_its_own() {
    let n = Node::start(DICTIONARY).await;
    let store = n
        .rules
        .state()
        .expect("rule state is on by default")
        .clone();
    // A connection that put a value: its dictionary holds `n` and one digit.
    let conn = n.rules.for_connection();
    let publish = |payload: &'static [u8]| {
        let _ = conn.on_publish(&PublishFacts {
            client: &ClientId("c1".into()),
            publisher: &Publisher::default(),
            topic: "t/1",
            payload: &Bytes::from_static(payload),
            qos: QoS::AtMostOnce,
            retain: false,
            app: &AppProperties::default(),
            message_expiry: None,
        });
    };
    publish(br#"{"v":7}"#);
    assert_eq!(store.bytes(), 2);
    for _ in 0..2 {
        let (status, body) = n
            .call(
                &n.operator,
                "POST",
                "/admin/v1/rules/test",
                Some(&json!({"topic": "t/1", "payload": "{\"v\":\"a dry run's value\"}"})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            body["results"],
            json!([
                {"rule": "a_put", "enabled": true, "result": "passed", "error": null,
                 "outputs": [{"action": "console",
                              "output": {"before": "undefined", "now": "a dry run's value"}}]},
                {"rule": "b_get", "enabled": true, "result": "passed", "error": null,
                 "outputs": [{"action": "console", "output": {"seen": "a dry run's value"}}]},
            ])
        );
        // The connection's dictionary is as it was, and nothing else is counted.
        assert_eq!(store.bytes(), 2);
    }
    publish(br#"{"v":"four"}"#);
    assert_eq!(store.bytes(), 5);
    drop(conn);
    assert_eq!(store.bytes(), 0);
}

// ---------------------------------------------------------------------------------------
// The state the rules keep (ADR 0085 D10)
// ---------------------------------------------------------------------------------------

/// Rules that keep state of every kind an operator can meet: a declared namespace with
/// string and binary keys and a counter, EMQX's table with keys of any type, a rule's
/// own namespace, and a connection's dictionary.
const KEEPING: &str = r#"[state.alerts]
max_keys = 4000
default_ttl = "1h"

[rules.put]
state = "alerts"
sql = '''SELECT kv_set(payload.k, payload.v) AS v FROM "set/#"'''
actions = [{ function = "console" }]

[rules.count]
state = "alerts"
sql = '''SELECT kv_incr(base64_decode(payload.k)) AS n FROM "bin/#"'''
actions = [{ function = "console" }]

[rules.emqx]
sql = '''SELECT kv_store_put(payload.k, payload.v) AS ok FROM "emqx/#"'''
actions = [{ function = "console" }]

[rules.own]
sql = '''SELECT kv_add('members', clientid) AS n FROM "own/#"'''
actions = [{ function = "console" }]

[rules.dict]
sql = '''SELECT proc_dict_put('k', payload.v) AS was FROM "dict/#"'''
actions = [{ function = "console" }]
"#;

/// `path` with `params` as its query string, percent-encoded.
fn with_query(path: &str, params: &[(&str, &str)]) -> String {
    let query: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{k}={}", mqttd::admin::http::percent_encode(v)))
        .collect();
    format!("{path}?{}", query.join("&"))
}

/// The namespace `name` of a `GET /admin/v1/rules/state` answer.
fn namespace<'a>(answer: &'a Value, name: &str) -> &'a Value {
    answer["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ns| ns["namespace"] == name)
        .unwrap_or_else(|| panic!("no namespace {name} in {answer}"))
}

/// Every page of `GET /admin/v1/rules/state/keys` for `params`, as `who`: the keys in
/// the order listed, and how many pages there were.
async fn list_keys(n: &Node, who: &Who, params: &[(&str, &str)]) -> (Vec<Value>, usize) {
    let (mut keys, mut pages, mut cursor) = (Vec::new(), 0, None::<String>);
    loop {
        let mut params = params.to_vec();
        if let Some(cursor) = &cursor {
            params.retain(|(name, _)| *name != "cursor");
            params.push(("cursor", cursor));
        }
        let path = with_query("/admin/v1/rules/state/keys", &params);
        let (status, body) = n.call(who, "GET", &path, None).await;
        assert_eq!(status, 200, "{body}");
        let page = body["keys"].as_array().unwrap();
        assert_eq!(body["count"], page.len());
        assert!(page.len() <= 1000, "a page holds at most 1,000 keys");
        keys.extend(page.iter().cloned());
        pages += 1;
        match body["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => return (keys, pages),
        }
        assert!(pages < 200, "the listing does not end");
    }
}

/// Take `bytes` and `expires_at` out of listed keys, after checking that each has
/// them: a size that covers the store's 128 bytes a key, and an expiry that is a time
/// or `null`.
fn without_size_and_expiry(keys: &mut [Value]) {
    for key in keys {
        let fields = key.as_object_mut().unwrap();
        let bytes = fields.remove("bytes").and_then(|b| b.as_u64());
        assert!(bytes.is_some_and(|b| b > 128), "{fields:?}");
        let expires = fields.remove("expires_at").expect("expires_at");
        assert!(
            expires.is_null() || expires.as_str().is_some_and(|t| t.len() == 24),
            "{expires}"
        );
    }
}

/// D10's roles. A viewer reads each namespace's mode, bounds and counts, and no key or
/// value: the overview holds none, and listing or reading keys is refused. An operator
/// lists keys and reads a value. A delete needs the operator role and a listed rules
/// writer, and is audited as `state.delete` with the key's hash, never the key.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one walk through every role and refusal
async fn a_viewer_reads_counts_an_operator_keys_and_values_and_a_writer_deletes() {
    let n = Node::start(KEEPING).await;
    n.publish(
        "set/1",
        br#"{"k":"hot:dev-1","v":{"temp":31.5,"serial":123456789012345678901234567890}}"#,
    );
    n.publish("set/1", br#"{"k":"hot:dev-2","v":"a secret reading"}"#);
    n.publish("emqx/1", br#"{"k":"dev-9","v":7}"#);
    n.publish("own/1", b"{}");
    let conn = n.rules.for_connection();
    let _ = conn.on_publish(&PublishFacts {
        client: &ClientId("c1".into()),
        publisher: &Publisher::default(),
        topic: "dict/1",
        payload: &Bytes::from_static(br#"{"v":"in a dictionary"}"#),
        qos: QoS::AtMostOnce,
        retain: false,
        app: &AppProperties::default(),
        message_expiry: None,
    });

    // The overview: a viewer's, and the same for an operator.
    let (status, overview) = n
        .call(&n.viewer, "GET", "/admin/v1/rules/state", None)
        .await;
    assert_eq!(status, 200, "{overview}");
    assert_eq!(overview["node"], "rules-node");
    assert_eq!(overview["enabled"], true);
    let names: Vec<&str> = overview["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|ns| ns["namespace"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["alerts", "kv_store", "rule:own"]);
    let alerts = namespace(&overview, "alerts");
    let held = alerts["bytes"].as_u64().unwrap();
    assert_eq!(
        *alerts,
        json!({
            "namespace": "alerts", "kind": "declared", "consistency": "local",
            "eviction": "fail", "max_keys": 4000, "max_bytes": 64 << 20,
            "default_ttl_secs": 3600, "max_ttl_secs": 30 * 24 * 3600,
            "keys": 2, "bytes": held, "calls": 2, "refused": 0,
            "refused_by": {"namespace_full": 0, "store_full": 0, "brownout": 0,
                           "wrong_type": 0, "refused": 0},
            "expired": 0, "expiry_backlog": 0, "evicted": 0, "deleted": 0,
        })
    );
    assert!(held > 2 * 128, "two keys and their values: {held}");
    let emqx = namespace(&overview, "kv_store");
    assert_eq!(
        (&emqx["kind"], &emqx["keys"], &emqx["default_ttl_secs"]),
        (&json!("kv_store"), &json!(1), &Value::Null)
    );
    assert_eq!(namespace(&overview, "rule:own")["kind"], "rule");
    let total = overview["store"]["bytes"].as_u64().unwrap();
    let dictionaries = overview["dictionaries"]["bytes"].as_u64().unwrap();
    assert_eq!(
        dictionaries,
        "k".len() as u64 + "in a dictionary".len() as u64
    );
    let in_namespaces: u64 = overview["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|ns| ns["bytes"].as_u64().unwrap())
        .sum();
    assert_eq!(total, in_namespaces + dictionaries);
    assert_eq!(
        (
            &overview["store"]["max_bytes"],
            &overview["store"]["brownout"]
        ),
        (&json!(256 << 20), &json!(false))
    );
    assert_eq!(overview["dictionaries"]["refused"], 0);
    // No key and no value, of any namespace or dictionary.
    let text = overview.to_string();
    for secret in [
        "hot:dev",
        "secret",
        "31.5",
        "1234567890",
        "dev-9",
        "members",
        "c1",
        "dictionary\"",
    ] {
        assert!(!text.contains(secret), "{secret} in {text}");
    }
    let (_, as_operator) = n
        .call(&n.operator, "GET", "/admin/v1/rules/state", None)
        .await;
    assert_eq!(as_operator, overview);

    // Keys and values are not a viewer's.
    let keys_path = with_query("/admin/v1/rules/state/keys", &[("ns", "alerts")]);
    let key_path = with_query(
        "/admin/v1/rules/state/key",
        &[("ns", "alerts"), ("key", "hot:dev-1")],
    );
    for (method, path) in [
        ("GET", &keys_path),
        ("GET", &key_path),
        ("DELETE", &key_path),
    ] {
        let (status, body) = n.call(&n.viewer, method, path, None).await;
        assert_eq!((status, code(&body)), (403, "forbidden"), "{method} {path}");
        assert!(!body.to_string().contains("secret"), "{body}");
    }

    // An operator lists the keys, each an object with its type, and reads one.
    let (status, body) = n.call(&n.operator, "GET", &keys_path, None).await;
    assert_eq!(status, 200, "{body}");
    let mut listed = body["keys"].as_array().unwrap().clone();
    listed.sort_by_key(|k| k["key"].as_str().unwrap().to_string());
    without_size_and_expiry(&mut listed);
    assert_eq!(
        listed,
        [
            json!({"key": "hot:dev-1", "key_encoding": "utf8", "type": "lww"}),
            json!({"key": "hot:dev-2", "key_encoding": "utf8", "type": "lww"}),
        ]
    );
    assert_eq!(
        (&body["namespace"], &body["node"], &body["next_cursor"]),
        (&json!("alerts"), &json!("rules-node"), &Value::Null)
    );
    let (status, body) = n.call(&n.operator, "GET", &key_path, None).await;
    assert_eq!(status, 200, "{body}");
    let expires = body["expires_at"]
        .as_str()
        .expect("default_ttl gave it one");
    assert!(expires.ends_with('Z') && expires.len() == 24, "{expires}");
    assert_eq!(
        body,
        json!({
            "node": "rules-node", "namespace": "alerts",
            "key": "hot:dev-1", "key_encoding": "utf8", "type": "lww",
            // What the namespace accounts it as: the store's 128 bytes, the key and
            // the value.
            "bytes": 128 + 9 + 53,
            // As the rule engine writes it: the integer keeps every digit.
            "value": "{\"temp\":31.5,\"serial\":123456789012345678901234567890}",
            "value_encoding": "utf8", "value_type": "map",
            "expires_at": expires,
        })
    );

    // A delete: not an operator's who is no listed writer, and the key stays.
    let (status, body) = n.call(&n.operator, "DELETE", &key_path, None).await;
    assert_eq!((status, code(&body)), (403, "not-a-rules-writer"), "{body}");
    assert_eq!(n.call(&n.operator, "GET", &key_path, None).await.0, 200);
    assert!(n.audit.of_kind("state.delete").is_empty());
    // A listed writer's.
    let (status, body) = n.call(&n.writer, "DELETE", &key_path, None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        json!({"node": "rules-node", "namespace": "alerts", "key": "hot:dev-1",
               "key_encoding": "utf8", "deleted": true})
    );
    let (status, body) = n.call(&n.operator, "GET", &key_path, None).await;
    assert_eq!((status, code(&body)), (404, "not-found"), "{body}");
    // Again: nothing was there, and that is said and recorded too.
    let (status, body) = n.call(&n.writer, "DELETE", &key_path, None).await;
    assert_eq!((status, &body["deleted"]), (200, &json!(false)), "{body}");
    let hash = sha256_hex("hot:dev-1");
    assert_eq!(
        n.audit.of_kind("state.delete"),
        [
            (
                Some("CN=writer".to_string()),
                format!("ns=alerts key_sha256={hash} key_bytes=9 deleted=true")
            ),
            (
                Some("CN=writer".to_string()),
                format!("ns=alerts key_sha256={hash} key_bytes=9 deleted=false")
            ),
        ]
    );
    // The audit log names the key by that hash everywhere, the requests' own records
    // included, and the refused ones.
    let requests = n.audit.of_kind("admin.request");
    assert!(
        requests.iter().any(|(who, detail)| {
            who.as_deref() == Some("CN=writer")
                && *detail
                    == format!(
                        "role=operator DELETE /admin/v1/rules/state/key?ns=alerts&\
                         key_sha256={hash}&key_bytes=9 -> 200"
                    )
        }),
        "{requests:?}"
    );
    assert!(
        requests.iter().all(|(_, detail)| !detail.contains("hot")),
        "{requests:?}"
    );
    let overview = n
        .call(&n.viewer, "GET", "/admin/v1/rules/state", None)
        .await
        .1;
    let alerts = namespace(&overview, "alerts");
    assert_eq!(
        (&alerts["keys"], &alerts["deleted"]),
        (&json!(1), &json!(1))
    );

    // With no writer listed, deletes are off, whoever asks.
    n.live.write().unwrap().rules.admin_writers.clear();
    let other = with_query(
        "/admin/v1/rules/state/key",
        &[("ns", "alerts"), ("key", "hot:dev-2")],
    );
    let (status, body) = n.call(&n.writer, "DELETE", &other, None).await;
    assert_eq!((status, code(&body)), (403, "rules-read-only"), "{body}");
    assert_eq!(n.call(&n.operator, "GET", &other, None).await.0, 200);
    drop(conn);
}

/// What the endpoints refuse: a namespace that is not there, a parameter left out or
/// malformed, a method they do not take.
#[tokio::test]
async fn the_state_endpoints_refuse_what_names_nothing() {
    let n = Node::start(KEEPING).await;
    n.publish("set/1", br#"{"k":"a","v":1}"#);
    let keys = "/admin/v1/rules/state/keys";
    let key = "/admin/v1/rules/state/key";
    for (method, path, status, expected) in [
        ("GET", keys.to_string(), 400, "bad-request"),
        ("GET", with_query(keys, &[("ns", "nope")]), 404, "not-found"),
        (
            "GET",
            with_query(keys, &[("ns", "alerts"), ("limit", "0")]),
            400,
            "bad-request",
        ),
        (
            "GET",
            with_query(keys, &[("ns", "alerts"), ("cursor", "not-a-cursor")]),
            400,
            "bad-request",
        ),
        (
            "GET",
            with_query(
                keys,
                &[("ns", "alerts"), ("cursor", "1-0000000000000000-0-")],
            ),
            409,
            "cursor-stale",
        ),
        (
            "GET",
            with_query(
                keys,
                &[
                    ("ns", "alerts"),
                    ("prefix", "!"),
                    ("prefix_encoding", "base64"),
                ],
            ),
            400,
            "bad-request",
        ),
        (
            "GET",
            with_query(key, &[("ns", "alerts")]),
            400,
            "bad-request",
        ),
        ("GET", with_query(key, &[("key", "a")]), 400, "bad-request"),
        (
            "GET",
            with_query(key, &[("ns", "nope"), ("key", "a")]),
            404,
            "not-found",
        ),
        (
            "GET",
            with_query(key, &[("ns", "alerts"), ("key", "missing")]),
            404,
            "not-found",
        ),
        (
            "GET",
            with_query(
                key,
                &[("ns", "alerts"), ("key", "AQ=="), ("key_encoding", "term")],
            ),
            400,
            "bad-request",
        ),
        (
            "DELETE",
            with_query(key, &[("ns", "nope"), ("key", "a")]),
            404,
            "not-found",
        ),
        (
            "POST",
            with_query(key, &[("ns", "alerts"), ("key", "a")]),
            405,
            "method-not-allowed",
        ),
        (
            "DELETE",
            "/admin/v1/rules/state".to_string(),
            405,
            "method-not-allowed",
        ),
    ] {
        let (got, body) = n.call(&n.writer, method, &path, None).await;
        assert_eq!(
            (got, code(&body)),
            (status, expected),
            "{method} {path}: {body}"
        );
    }
    // A delete that named no namespace's key recorded no `state.delete`.
    assert!(n.audit.of_kind("state.delete").is_empty());
}

/// Paging over the wire: 2,500 keys come in pages of at most 1,000, each key once, and
/// a prefix and a smaller `limit` page the same way.
#[tokio::test]
async fn the_keys_of_a_namespace_are_listed_in_pages_of_at_most_a_thousand() {
    let n = Node::start(KEEPING).await;
    for i in 0..2_500 {
        let payload = format!(r#"{{"k":"dev:{i:04}","v":{i}}}"#);
        n.publish("set/1", payload.as_bytes());
    }
    n.publish("set/1", br#"{"k":"other","v":0}"#);
    // A page is 100 keys unless `limit` says otherwise, and never more than 1,000.
    let first = with_query("/admin/v1/rules/state/keys", &[("ns", "alerts")]);
    let (_, page) = n.call(&n.operator, "GET", &first, None).await;
    assert_eq!(page["count"], 100, "{page}");
    let (keys, pages) = list_keys(&n, &n.operator, &[("ns", "alerts"), ("limit", "5000")]).await;
    assert_eq!(keys.len(), 2_501);
    assert!((3..=4).contains(&pages), "{pages} pages");
    let distinct: std::collections::BTreeSet<&str> =
        keys.iter().map(|k| k["key"].as_str().unwrap()).collect();
    assert_eq!(distinct.len(), 2_501, "no key is listed twice");
    assert!(keys.iter().all(|k| k["type"] == "lww"));
    let (some, pages) = list_keys(
        &n,
        &n.operator,
        &[("ns", "alerts"), ("prefix", "dev:00"), ("limit", "30")],
    )
    .await;
    assert_eq!(some.len(), 100);
    assert!(pages >= 4, "{pages} pages of at most 30");
    assert!(some
        .iter()
        .all(|k| k["key"].as_str().unwrap().starts_with("dev:00")));
    // A key deleted and a key written between two pages: neither is listed twice, and
    // every key that stayed is listed.
    let first = with_query(
        "/admin/v1/rules/state/keys",
        &[("ns", "alerts"), ("limit", "1000")],
    );
    let (_, page) = n.call(&n.operator, "GET", &first, None).await;
    let cursor = page["next_cursor"].as_str().unwrap().to_string();
    let mut seen: Vec<String> = page["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k["key"].as_str().unwrap().to_string())
        .collect();
    let gone = seen[0].clone();
    let path = with_query(
        "/admin/v1/rules/state/key",
        &[("ns", "alerts"), ("key", &gone)],
    );
    assert_eq!(n.call(&n.writer, "DELETE", &path, None).await.0, 200);
    for i in 0..500 {
        let payload = format!(r#"{{"k":"new:{i}","v":{i}}}"#);
        n.publish("set/1", payload.as_bytes());
    }
    let (rest, _) = list_keys(&n, &n.operator, &[("ns", "alerts"), ("cursor", &cursor)]).await;
    seen.extend(rest.iter().map(|k| k["key"].as_str().unwrap().to_string()));
    let distinct: std::collections::BTreeSet<&String> = seen.iter().collect();
    assert_eq!(distinct.len(), seen.len(), "no key is listed twice");
    assert!((0..2_500).all(|i| distinct.contains(&format!("dev:{i:04}"))));
}

/// The representation round-trips over the wire. Whatever a key is — text with spaces,
/// separators and control characters, bytes that are not UTF-8, and in EMQX's table a
/// number, a boolean, `null`, an array, a map — the `key` and `key_encoding` a listing
/// shows read that key and delete it. And a value reads as the rule engine writes it,
/// with what it is.
#[tokio::test]
#[allow(clippy::too_many_lines)] // every kind of key and value, once
async fn every_key_a_listing_shows_can_be_read_and_deleted_as_shown() {
    let n = Node::start(KEEPING).await;
    for payload in [
        r#"{"k":"plain","v":"1"}"#,
        r#"{"k":" sp ace/&?=+%#","v":1}"#,
        r#"{"k":"ctl\u001b[2J\r\n\u009b","v":1.0}"#,
        r#"{"k":"ключ-🔑","v":[1,"x",null]}"#,
        r#"{"k":"big","v":123456789012345678901234567890}"#,
        r#"{"k":"esc","v":"line\u001b[2J"}"#,
        r#"{"k":"flag","v":true}"#,
    ] {
        n.publish("set/1", payload.as_bytes());
    }
    // A key that is not UTF-8, counted three times.
    for _ in 0..3 {
        n.publish("bin/1", br#"{"k":"/wD+"}"#);
    }
    // EMQX's table: `1`, `1.0` and `'1'` are three keys.
    for payload in [
        r#"{"k":"dev-1","v":"a string key"}"#,
        r#"{"k":"1","v":"the string 1"}"#,
        r#"{"k":1,"v":"the integer 1"}"#,
        r#"{"k":1.0,"v":"the float 1.0"}"#,
        r#"{"k":true,"v":"true"}"#,
        r#"{"k":null,"v":"null"}"#,
        r#"{"k":[1,"x"],"v":"an array"}"#,
        r#"{"k":{"b":1,"a":2},"v":"a map"}"#,
        r#"{"v":"the key undefined"}"#,
        r#"{"k":"no value"}"#,
    ] {
        n.publish("emqx/1", payload.as_bytes());
    }
    n.publish("own/1", b"{}");

    let read = |ns: &'static str, shown: Value| {
        let n = &n;
        async move {
            let path = with_query(
                "/admin/v1/rules/state/key",
                &[
                    ("ns", ns),
                    ("key", shown["key"].as_str().unwrap()),
                    ("key_encoding", shown["key_encoding"].as_str().unwrap()),
                ],
            );
            let (status, body) = n.call(&n.operator, "GET", &path, None).await;
            assert_eq!(status, 200, "{shown}: {body}");
            // The answer names the key as the listing did.
            for field in ["key", "key_encoding", "type"] {
                assert_eq!(body[field], shown[field], "{shown}: {body}");
            }
            assert_eq!(body.get("term"), shown.get("term"), "{shown}: {body}");
            (path, body)
        }
    };

    // -- a declared namespace
    let (keys, _) = list_keys(&n, &n.operator, &[("ns", "alerts")]).await;
    let mut values = std::collections::BTreeMap::new();
    for shown in &keys {
        let (_, body) = read("alerts", shown.clone()).await;
        values.insert(
            (
                shown["key"].as_str().unwrap().to_string(),
                shown["key_encoding"].as_str().unwrap().to_string(),
            ),
            (
                body["type"].as_str().unwrap().to_string(),
                body["value"].as_str().unwrap().to_string(),
                body["value_encoding"].as_str().unwrap().to_string(),
                body["value_type"].as_str().unwrap().to_string(),
            ),
        );
    }
    let expected: std::collections::BTreeMap<_, _> = [
        (("plain", "utf8"), ("lww", "1", "utf8", "string")),
        ((" sp ace/&?=+%#", "utf8"), ("lww", "1", "utf8", "integer")),
        (
            ("ctl\u{1b}[2J\r\n\u{9b}", "utf8"),
            ("lww", "1.0", "utf8", "float"),
        ),
        (
            ("ключ-🔑", "utf8"),
            ("lww", "[1,\"x\",null]", "utf8", "array"),
        ),
        (
            ("big", "utf8"),
            ("lww", "123456789012345678901234567890", "utf8", "integer"),
        ),
        (("esc", "utf8"), ("lww", "line\u{1b}[2J", "utf8", "string")),
        (("flag", "utf8"), ("lww", "true", "utf8", "boolean")),
        // The bytes ff 00 fe, and the counter's count.
        (("/wD+", "base64"), ("sum", "3", "utf8", "integer")),
    ]
    .into_iter()
    .map(|((k, ke), (t, v, ve, vt))| {
        (
            (k.to_string(), ke.to_string()),
            (t.to_string(), v.to_string(), ve.to_string(), vt.to_string()),
        )
    })
    .collect();
    assert_eq!(values, expected);

    // -- a rule's own namespace: a set reads as its elements
    let (mut keys, _) = list_keys(&n, &n.operator, &[("ns", "rule:own")]).await;
    without_size_and_expiry(&mut keys);
    assert_eq!(
        keys,
        [json!({"key": "members", "key_encoding": "utf8", "type": "union"})]
    );
    let (_, body) = read("rule:own", keys[0].clone()).await;
    assert_eq!(
        (&body["value"], &body["value_type"]),
        (&json!("[\"c1\"]"), &json!("array"))
    );

    // -- EMQX's table: a key of any type
    let (keys, _) = list_keys(&n, &n.operator, &[("ns", "kv_store")]).await;
    let mut shown = std::collections::BTreeMap::new();
    for key in &keys {
        let (_, body) = read("kv_store", key.clone()).await;
        let name = match key["key_encoding"].as_str().unwrap() {
            "term" => format!("term {}", key["term"].as_str().unwrap()),
            encoding => format!("{encoding} {}", key["key"].as_str().unwrap()),
        };
        shown.insert(
            name,
            (
                body["value"].as_str().unwrap().to_string(),
                body["value_type"].as_str().unwrap().to_string(),
            ),
        );
    }
    let expected: std::collections::BTreeMap<String, (String, String)> = [
        ("utf8 dev-1", "a string key", "string"),
        ("utf8 1", "the string 1", "string"),
        ("term 1", "the integer 1", "string"),
        ("term 1.0", "the float 1.0", "string"),
        ("term true", "true", "string"),
        ("term null", "null", "string"),
        ("term [1,\"x\"]", "an array", "string"),
        ("term {\"a\":2,\"b\":1}", "a map", "string"),
        ("term undefined", "the key undefined", "string"),
        // EMQX's table stores `undefined`.
        ("utf8 no value", "undefined", "undefined"),
    ]
    .into_iter()
    .map(|(k, v, t)| (k.to_string(), (v.to_string(), t.to_string())))
    .collect();
    assert_eq!(shown, expected);
    // A prefix there is of a string key's text.
    let (mut dev, _) = list_keys(&n, &n.operator, &[("ns", "kv_store"), ("prefix", "dev")]).await;
    without_size_and_expiry(&mut dev);
    assert_eq!(
        dev,
        [json!({"key": "dev-1", "key_encoding": "utf8", "type": "lww"})]
    );

    // -- and every one of them is deleted as it was shown
    for ns in ["alerts", "kv_store", "rule:own"] {
        let (keys, _) = list_keys(&n, &n.operator, &[("ns", ns)]).await;
        assert!(!keys.is_empty());
        for key in keys {
            let (path, _) = read(ns, key.clone()).await;
            let (status, body) = n.call(&n.writer, "DELETE", &path, None).await;
            assert_eq!(
                (status, &body["deleted"]),
                (200, &json!(true)),
                "{key}: {body}"
            );
            assert_eq!(
                n.call(&n.operator, "GET", &path, None).await.0,
                404,
                "{key}"
            );
        }
        let (keys, _) = list_keys(&n, &n.operator, &[("ns", ns)]).await;
        assert_eq!(keys, Vec::<Value>::new(), "{ns}");
    }
    // The audit log holds none of the keys, in a delete's record or a request's.
    for kind in ["state.delete", "admin.request"] {
        for (_, detail) in n.audit.of_kind(kind) {
            for key in ["plain", "sp%20ace", "ctl", "wD", "dev-1", "members"] {
                assert!(!detail.contains(key), "{key} in {detail}");
            }
        }
    }
    assert_eq!(n.audit.of_kind("state.delete").len(), 8 + 10 + 1);
}

/// D10's metrics, each exported for a namespace the rules hold: keys, bytes, calls by
/// operation and result, expiries and the expiry backlog; and the dictionaries'. No
/// label is a key.
#[tokio::test]
async fn the_state_metrics_are_exported_by_namespace() {
    let n = Node::start(KEEPING).await;
    let state_series = |n: &Node| -> Vec<String> {
        n.rules.export_state_metrics();
        n.metrics
            .render()
            .lines()
            .filter(|l| l.starts_with("mqttd_rule_state_"))
            .map(String::from)
            .collect()
    };
    // Before a call: the namespaces are there, at zero.
    let series = state_series(&n);
    for line in [
        "mqttd_rule_state_keys{namespace=\"alerts\"} 0",
        "mqttd_rule_state_bytes{namespace=\"alerts\"} 0",
        "mqttd_rule_state_expired_total{namespace=\"alerts\"} 0",
        "mqttd_rule_state_expiry_backlog{namespace=\"alerts\"} 0",
        "mqttd_rule_state_keys{namespace=\"kv_store\"} 0",
        "mqttd_rule_state_keys{namespace=\"rule:own\"} 0",
        "mqttd_rule_state_dictionary_bytes 0",
        "mqttd_rule_state_dictionary_refused_total 0",
    ] {
        assert!(
            series.iter().any(|l| l == line),
            "missing {line}: {series:#?}"
        );
    }
    assert!(
        !series
            .iter()
            .any(|l| l.starts_with("mqttd_rule_state_ops_total")),
        "a call is a series once it happened: {series:#?}"
    );
    n.publish("set/1", br#"{"k":"hot:dev-1","v":1}"#);
    n.publish("set/1", br#"{"k":"hot:dev-1","v":2}"#);
    n.publish("set/1", br#"{"k":"hot:dev-2","v":2}"#);
    // The key holds a value `kv_set` wrote: `kv_incr` on it is another type's call.
    n.publish("bin/1", br#"{"k":"aG90OmRldi0x"}"#);
    n.publish("emqx/1", br#"{"k":"p","v":1}"#);
    let series = state_series(&n);
    let bytes = n.rules.state().unwrap().counts("alerts").unwrap().bytes;
    for line in [
        "mqttd_rule_state_keys{namespace=\"alerts\"} 2".to_string(),
        format!("mqttd_rule_state_bytes{{namespace=\"alerts\"}} {bytes}"),
        "mqttd_rule_state_ops_total{namespace=\"alerts\",op=\"set\",result=\"ok\"} 3".to_string(),
        "mqttd_rule_state_ops_total{namespace=\"alerts\",op=\"incr\",result=\"wrong_type\"} 1"
            .to_string(),
        "mqttd_rule_state_ops_total{namespace=\"kv_store\",op=\"set\",result=\"ok\"} 1".to_string(),
    ] {
        assert!(series.contains(&line), "missing {line}: {series:#?}");
    }
    assert_eq!(
        series
            .iter()
            .filter(|l| l.starts_with("mqttd_rule_state_ops_total"))
            .count(),
        3,
        "{series:#?}"
    );
    // Exporting again adds nothing: a counter moves by what happened since.
    assert_eq!(state_series(&n), series);
    // Bounded labels: a namespace, an operation, a result. Never a key or a value.
    let text = series.join("\n");
    assert!(!text.contains("hot") && !text.contains("dev-"), "{text}");
    // A namespace the rules no longer hold: its gauges go, its counters stay.
    let without = KEEPING.replace("[rules.emqx]", "[rules.emqx]\nenable = true");
    let without = without.replace("kv_store_put(payload.k, payload.v)", "payload.k");
    std::fs::write(&n.file, without).unwrap();
    assert!(n.reloader.reload_with_outcome("test").applied);
    let series = state_series(&n);
    for gauge in ["keys", "bytes", "expiry_backlog"] {
        let gone = format!("mqttd_rule_state_{gauge}{{namespace=\"kv_store\"}}");
        assert!(!series.iter().any(|l| l.starts_with(&gone)), "{series:#?}");
    }
    assert!(series
        .iter()
        .any(|l| l
            == "mqttd_rule_state_ops_total{namespace=\"kv_store\",op=\"set\",result=\"ok\"} 1"));
    assert!(series
        .iter()
        .any(|l| l == "mqttd_rule_state_keys{namespace=\"alerts\"} 2"));
}

/// D10: the trace records a failed state call as its rule's error: the function, the
/// store's reason, and what happened. The error holds no key and no value.
#[tokio::test]
async fn the_trace_carries_a_failed_state_calls_reason() {
    let rules = KEEPING.replace("max_keys = 4000", "max_keys = 1");
    let mut n = Node::start(&rules).await;
    let next = |n: &mut Node, topic: &str, payload: &[u8]| {
        n.publish(topic, payload);
        let record = n.trace_rx.try_recv().expect("the rule's trace record");
        assert!(matches!(
            n.trace_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        let doc: Value =
            serde_json::from_str(&mqttd::rules_sys::record_json("rules-node", &record)).unwrap();
        doc
    };
    let doc = next(&mut n, "set/1", br#"{"k":"first-key","v":"kept"}"#);
    assert_eq!(
        (&doc["result"], &doc["error"]),
        (&json!("passed"), &Value::Null)
    );
    // A second key does not fit a namespace of one.
    let doc = next(&mut n, "set/1", br#"{"k":"second-key","v":"refused"}"#);
    assert_eq!(
        (&doc["rule"], &doc["result"]),
        (&json!("put"), &json!("failed"))
    );
    let bytes = n.rules.state().unwrap().counts("alerts").unwrap().bytes;
    assert_eq!(
        doc["error"],
        format!(
            "kv_set(): namespace_full: namespace `alerts` holds 1 keys and {bytes} bytes (max_keys 1, \
             max_bytes 67108864); a new key does not fit"
        )
    );
    // Another type's operation on the key that is there.
    let doc = next(&mut n, "bin/1", br#"{"k":"Zmlyc3Qta2V5"}"#);
    assert_eq!(
        (&doc["rule"], &doc["result"]),
        (&json!("count"), &json!("failed"))
    );
    assert_eq!(
        doc["error"],
        "kv_incr(): wrong_type: the key holds a value of type `lww`, and this is an operation \
         on type `sum` (kv_del deletes a key of any type)"
    );
    // A call the rule engine refuses before the store sees it reads the same way.
    let doc = next(&mut n, "set/1", br#"{"k":"","v":1}"#);
    assert_eq!(
        doc["error"],
        "kv_set(): bad_key: a key is 1 to 256 bytes, got 0"
    );
    // The statistics keep the kind and the time, and the admin API shows the text to
    // an operator only.
    let (_, as_viewer) = n.call(&n.viewer, "GET", "/admin/v1/rules", None).await;
    let last = &rule(&as_viewer, "count")["last_error"];
    assert_eq!(last["kind"], "sql");
    assert!(last.get("message").is_none(), "{last}");
    let (_, as_operator) = n.call(&n.operator, "GET", "/admin/v1/rules", None).await;
    assert!(rule(&as_operator, "count")["last_error"]["message"]
        .as_str()
        .unwrap()
        .starts_with("kv_incr(): wrong_type: "));
}

/// D10 for an `eventual` namespace, over the wire: the overview says how its
/// replication stands, its keys are listed and read as a `local` one's, and a writer's
/// delete is a replicated delete: the key reads as absent at once, and stays counted,
/// as a tombstone, for the namespace's tombstone window.
#[tokio::test]
async fn an_eventual_namespace_is_seen_and_managed_like_a_local_one() {
    let rules = KEEPING.replace(
        "[state.alerts]",
        "[state.alerts]\nconsistency = \"eventual\"\ntombstone_window = \"2h\"",
    );
    let n = Node::start(&rules).await;
    n.publish("set/1", br#"{"k":"hot:dev-1","v":{"temp":31.5}}"#);
    n.publish("set/1", br#"{"k":"hot:dev-2","v":"two"}"#);
    n.publish("bin/1", br#"{"k":"/wD+"}"#);
    let (status, overview) = n
        .call(&n.viewer, "GET", "/admin/v1/rules/state", None)
        .await;
    assert_eq!(status, 200, "{overview}");
    let alerts = namespace(&overview, "alerts");
    assert_eq!(
        (
            &alerts["consistency"],
            &alerts["tombstone_window_secs"],
            &alerts["keys"],
            &alerts["calls"]
        ),
        (&json!("eventual"), &json!(7200), &json!(3), &json!(3))
    );
    assert_eq!(
        alerts["replication"],
        json!({
            "links": 0, "pending": 0, "lag_ms": 0, "merged": 0,
            "refused_by": {"declaration": 0, "clock_skew": 0, "namespace_full": 0,
                           "store_full": 0, "brownout": 0, "malformed": 0},
        })
    );
    // A `local` namespace has neither field.
    n.publish("own/1", b"{}");
    let (_, overview) = n
        .call(&n.viewer, "GET", "/admin/v1/rules/state", None)
        .await;
    let own = namespace(&overview, "rule:own");
    assert!(
        own.get("replication").is_none() && own.get("tombstone_window_secs").is_none(),
        "{own}"
    );

    let (mut keys, _) = list_keys(&n, &n.operator, &[("ns", "alerts")]).await;
    keys.sort_by_key(|k| k["key"].as_str().unwrap().to_string());
    without_size_and_expiry(&mut keys);
    assert_eq!(
        keys,
        [
            json!({"key": "/wD+", "key_encoding": "base64", "type": "sum"}),
            json!({"key": "hot:dev-1", "key_encoding": "utf8", "type": "lww"}),
            json!({"key": "hot:dev-2", "key_encoding": "utf8", "type": "lww"}),
        ]
    );
    let path = with_query(
        "/admin/v1/rules/state/key",
        &[("ns", "alerts"), ("key", "hot:dev-1")],
    );
    let (status, body) = n.call(&n.operator, "GET", &path, None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (&body["type"], &body["value"], &body["value_type"]),
        (&json!("lww"), &json!("{\"temp\":31.5}"), &json!("map"))
    );
    assert!(body["expires_at"].is_string(), "{body}");

    let (status, body) = n.call(&n.operator, "DELETE", &path, None).await;
    assert_eq!((status, code(&body)), (403, "not-a-rules-writer"), "{body}");
    let (status, body) = n.call(&n.writer, "DELETE", &path, None).await;
    assert_eq!((status, &body["deleted"]), (200, &json!(true)), "{body}");
    assert_eq!(n.call(&n.operator, "GET", &path, None).await.0, 404);
    let (status, body) = n.call(&n.writer, "DELETE", &path, None).await;
    assert_eq!((status, &body["deleted"]), (200, &json!(false)), "{body}");
    let hash = sha256_hex("hot:dev-1");
    assert_eq!(
        n.audit.of_kind("state.delete"),
        [
            (
                Some("CN=writer".to_string()),
                format!("ns=alerts key_sha256={hash} key_bytes=9 deleted=true")
            ),
            (
                Some("CN=writer".to_string()),
                format!("ns=alerts key_sha256={hash} key_bytes=9 deleted=false")
            ),
        ]
    );
    // Not listed any more; still a key the namespace counts, until its window is over.
    let (keys, _) = list_keys(&n, &n.operator, &[("ns", "alerts")]).await;
    assert_eq!(keys.len(), 2);
    let (_, overview) = n
        .call(&n.viewer, "GET", "/admin/v1/rules/state", None)
        .await;
    let alerts = namespace(&overview, "alerts");
    assert_eq!(
        (&alerts["keys"], &alerts["deleted"], &alerts["calls"]),
        (&json!(3), &json!(1), &json!(3))
    );
    // A rule sees the delete as its own `kv_del`: the key is new again.
    n.publish("set/1", br#"{"k":"hot:dev-1","v":"again"}"#);
    let (status, body) = n.call(&n.operator, "GET", &path, None).await;
    assert_eq!((status, &body["value"]), (200, &json!("again")), "{body}");
}

// ---------------------------------------------------------------------------------------
// Looking into the store: sizes, expiries, values and filters (ADR 0085 T12)
// ---------------------------------------------------------------------------------------

/// Rules that write keys with an expiry the publisher chooses and with the
/// namespace's default, into a `local` namespace, an `eventual` one, a rule's own and
/// EMQX's table.
const EXPIRING: &str = r#"[state.fleet]
max_keys = 4000

[state.shared]
consistency = "eventual"

[rules.soon]
state = "fleet"
sql = '''SELECT kv_set(payload.k, payload.v, payload.ttl) AS v FROM "soon/#"'''
actions = [{ function = "console" }]

[rules.later]
state = "fleet"
sql = '''SELECT kv_set(payload.k, payload.v) AS v FROM "later/#"'''
actions = [{ function = "console" }]

[rules.shared_soon]
state = "shared"
sql = '''SELECT kv_incr(payload.k, 1, payload.ttl) AS n FROM "shared/soon/#"'''
actions = [{ function = "console" }]

[rules.shared_later]
state = "shared"
sql = '''SELECT kv_set(payload.k, payload.v) AS v FROM "shared/later/#"'''
actions = [{ function = "console" }]

[rules.emqx]
sql = '''SELECT kv_store_put(payload.k, payload.v) AS ok FROM "emqx/#"'''
actions = [{ function = "console" }]

[rules.own]
sql = '''SELECT kv_add(payload.k, clientid, '5m') AS n FROM "own/#"'''
actions = [{ function = "console" }]
"#;

/// The `key` of every listed key, sorted, after checking that none came twice.
fn names(keys: &[Value]) -> Vec<String> {
    let names: Vec<String> = keys
        .iter()
        .map(|k| k["key"].as_str().unwrap().to_string())
        .collect();
    let distinct: std::collections::BTreeSet<&String> = names.iter().collect();
    assert_eq!(distinct.len(), names.len(), "no key is listed twice");
    distinct.into_iter().cloned().collect()
}

/// A time `secs` from now, as the API writes one.
fn in_secs(secs: u64) -> String {
    mqttd::rules_sys::rfc3339_millis(
        std::time::SystemTime::now() + std::time::Duration::from_secs(secs),
    )
}

/// T12: the listing says of every key its type, what the namespace accounts it as and
/// when it expires, in a declared namespace (`local` and `eventual`), a rule's own and
/// EMQX's table; the sizes listed add up to what the overview counts. Values come only
/// with `values=true`, as the read of one key shows them, and only to an operator.
#[tokio::test]
#[allow(clippy::too_many_lines)] // every kind of namespace, once
async fn a_listing_shows_each_keys_type_size_and_expiry_and_values_when_asked() {
    let n = Node::start(EXPIRING).await;
    let (before_soon, before_day) = (in_secs(60), in_secs(86_400));
    n.publish("soon/1", br#"{"k":"hot:dev-1","v":{"temp":31.5},"ttl":60}"#);
    n.publish("later/1", br#"{"k":"hot:dev-2","v":"a secret reading"}"#);
    n.publish("shared/soon/1", br#"{"k":"seen:dev-1","ttl":"1m"}"#);
    n.publish("shared/later/1", br#"{"k":"last:dev-1","v":7}"#);
    n.publish("emqx/1", br#"{"k":"dev-9","v":[1,"x"]}"#);
    n.publish("emqx/1", br#"{"k":1.0,"v":"a float key"}"#);
    n.publish("own/1", br#"{"k":"members"}"#);
    let (after_soon, after_day) = (in_secs(60), in_secs(86_400));

    let listed = |ns: &'static str, values: bool| {
        let n = &n;
        async move {
            let mut params = vec![("ns", ns)];
            if values {
                params.push(("values", "true"));
            }
            let (mut keys, _) = list_keys(n, &n.operator, &params).await;
            keys.sort_by_key(|k| k["key"].as_str().unwrap().to_string());
            keys
        }
    };
    let between = |key: &Value, from: &str, to: &str| {
        let at = key["expires_at"].as_str().unwrap();
        assert!(from <= at && at <= to, "{from} <= {at} <= {to}");
    };

    // A declared namespace: an expiry a rule chose, and the namespace's default.
    let fleet = listed("fleet", false).await;
    assert_eq!(names(&fleet), ["hot:dev-1", "hot:dev-2"]);
    between(&fleet[0], &before_soon, &after_soon);
    between(&fleet[1], &before_day, &after_day);
    for key in &fleet {
        assert_eq!(key["type"], "lww");
        // No value, and nothing of one, unless it is asked for.
        let fields: Vec<&String> = key.as_object().unwrap().keys().collect();
        assert_eq!(
            fields,
            ["bytes", "expires_at", "key", "key_encoding", "type"]
        );
    }
    let size = |key: &Value| key["bytes"].as_u64().unwrap();
    assert_eq!(size(&fleet[0]), 128 + 9 + r#"{"temp":31.5}"#.len() as u64);
    // The sizes are the namespace's own account, in every kind of namespace.
    let (_, overview) = n
        .call(&n.viewer, "GET", "/admin/v1/rules/state", None)
        .await;
    for ns in ["fleet", "kv_store", "rule:own"] {
        let keys = listed(ns, false).await;
        assert_eq!(
            keys.iter().map(size).sum::<u64>(),
            namespace(&overview, ns)["bytes"].as_u64().unwrap(),
            "{ns}"
        );
    }

    // With values: what the read of one key shows, beside the same facts.
    let valued = listed("fleet", true).await;
    for (plain, with) in fleet.iter().zip(&valued) {
        let path = with_query(
            "/admin/v1/rules/state/key",
            &[("ns", "fleet"), ("key", plain["key"].as_str().unwrap())],
        );
        let (status, one) = n.call(&n.operator, "GET", &path, None).await;
        assert_eq!(status, 200, "{one}");
        for field in [
            "value",
            "value_encoding",
            "value_type",
            "type",
            // The read of one key says its size too, and it is the listing's.
            "bytes",
            "expires_at",
        ] {
            assert_eq!(with[field], one[field], "{field}: {with}");
        }
        assert_eq!(with["bytes"], plain["bytes"]);
    }
    assert_eq!(
        (&valued[0]["value"], &valued[0]["value_type"]),
        (&json!("{\"temp\":31.5}"), &json!("map"))
    );
    assert_eq!(valued[1]["value"], "a secret reading");

    // An `eventual` namespace: the same, for a counter and a value.
    let shared = listed("shared", true).await;
    assert_eq!(names(&shared), ["last:dev-1", "seen:dev-1"]);
    assert_eq!(
        (
            &shared[0]["type"],
            &shared[0]["value"],
            &shared[0]["value_type"]
        ),
        (&json!("lww"), &json!("7"), &json!("integer"))
    );
    assert_eq!(
        (&shared[1]["type"], &shared[1]["value"]),
        (&json!("sum"), &json!("1"))
    );
    between(&shared[0], &before_day, &after_day);
    between(&shared[1], &before_soon, &after_soon);
    assert!(shared.iter().all(|key| size(key) > 128), "{shared:?}");

    // A rule's own namespace.
    let own = listed("rule:own", true).await;
    assert_eq!(
        (
            &own[0]["key"],
            &own[0]["type"],
            &own[0]["value"],
            &own[0]["value_type"]
        ),
        (
            &json!("members"),
            &json!("union"),
            &json!("[\"c1\"]"),
            &json!("array")
        )
    );
    between(&own[0], &in_secs(290), &in_secs(300));

    // EMQX's table: any key, any value, no expiry.
    let emqx = listed("kv_store", true).await;
    assert_eq!(emqx.len(), 2);
    let float = emqx.iter().find(|k| k["key_encoding"] == "term").unwrap();
    assert_eq!(
        (&float["term"], &float["value"], &float["expires_at"]),
        (&json!("1.0"), &json!("a float key"), &Value::Null)
    );
    let text = emqx.iter().find(|k| k["key"] == "dev-9").unwrap();
    assert_eq!(
        (
            &text["value"],
            &text["value_type"],
            &text["expires_at"],
            &text["type"]
        ),
        (
            &json!("[1,\"x\"]"),
            &json!("array"),
            &Value::Null,
            &json!("lww")
        )
    );

    // Not a viewer's, with values or without: refused, with nothing of a key in it.
    for params in [
        &[("ns", "fleet")][..],
        &[("ns", "fleet"), ("values", "true")],
        &[
            ("ns", "kv_store"),
            ("no_expiry", "true"),
            ("values", "true"),
        ],
    ] {
        let path = with_query("/admin/v1/rules/state/keys", params);
        let (status, body) = n.call(&n.viewer, "GET", &path, None).await;
        assert_eq!((status, code(&body)), (403, "forbidden"), "{path}");
        let text = body.to_string();
        assert!(!text.contains("secret") && !text.contains("dev-"), "{text}");
    }
}

/// T12: `expires_within` and `no_expiry`, alone and combined with `prefix`, select
/// exactly the keys written that way, in a `local` namespace, an `eventual` one and
/// EMQX's table, where no key expires, so `no_expiry` selects every key and
/// `expires_within` none. The two together are refused, as is a duration that is not
/// one. A key written between two pages of a filtered listing is listed when the
/// listing has yet to reach its place, and no key is listed twice.
#[tokio::test]
#[allow(clippy::too_many_lines)] // each filter and each combination, once
async fn the_filters_select_keys_by_expiry_alone_and_with_a_prefix() {
    let n = Node::start(EXPIRING).await;
    for i in 0..300 {
        let soon = format!(r#"{{"k":"dev:{i:03}:soon","v":{i},"ttl":"5m"}}"#);
        n.publish("soon/1", soon.as_bytes());
        let hour = format!(r#"{{"k":"dev:{i:03}:hour","v":{i},"ttl":3600}}"#);
        n.publish("soon/1", hour.as_bytes());
        let later = format!(r#"{{"k":"dev:{i:03}:day","v":{i}}}"#);
        n.publish("later/1", later.as_bytes());
    }
    for i in 0..50 {
        let soon = format!(r#"{{"k":"seen:{i:02}","ttl":120}}"#);
        n.publish("shared/soon/1", soon.as_bytes());
        let later = format!(r#"{{"k":"last:{i:02}","v":{i}}}"#);
        n.publish("shared/later/1", later.as_bytes());
        let emqx = format!(r#"{{"k":"dev-{i:02}","v":{i}}}"#);
        n.publish("emqx/1", emqx.as_bytes());
    }
    n.publish("emqx/1", br#"{"k":[1,"x"],"v":"an array key"}"#);
    let select = |params: &'static [(&'static str, &'static str)]| {
        let n = &n;
        async move { names(&list_keys(n, &n.operator, params).await.0) }
    };
    let ends = |keys: &[String], kind: &str, count: usize| {
        assert_eq!(keys.len(), count, "{kind}");
        assert!(keys.iter().all(|k| k.ends_with(kind)), "{kind}: {keys:?}");
    };

    // A `local` namespace. Every key there has an expiry: a rule's, or the default.
    assert_eq!(
        select(&[("ns", "fleet"), ("limit", "1000")]).await.len(),
        900
    );
    ends(
        &select(&[("ns", "fleet"), ("expires_within", "10m")]).await,
        ":soon",
        300,
    );
    ends(
        &select(&[("ns", "fleet"), ("expires_within", "600")]).await,
        ":soon",
        300,
    );
    assert_eq!(
        select(&[("ns", "fleet"), ("expires_within", "4m")])
            .await
            .len(),
        0
    );
    let two = select(&[("ns", "fleet"), ("expires_within", "2h"), ("limit", "70")]).await;
    assert_eq!(two.len(), 600);
    assert!(two.iter().all(|k| !k.ends_with(":day")));
    assert_eq!(
        select(&[("ns", "fleet"), ("expires_within", "2d")])
            .await
            .len(),
        900
    );
    assert_eq!(
        select(&[("ns", "fleet"), ("no_expiry", "true")])
            .await
            .len(),
        0
    );
    // With a prefix: both must hold.
    let some = select(&[
        ("ns", "fleet"),
        ("expires_within", "10m"),
        ("prefix", "dev:00"),
    ])
    .await;
    ends(&some, ":soon", 10);
    assert!(some.iter().all(|k| k.starts_with("dev:00")));
    let encoded = select(&[
        ("ns", "fleet"),
        ("expires_within", "2h"),
        ("prefix", "ZGV2OjAx"),
        ("prefix_encoding", "base64"),
        ("limit", "7"),
    ])
    .await;
    assert_eq!(encoded.len(), 20);
    assert!(encoded.iter().all(|k| k.starts_with("dev:01")));
    assert_eq!(
        select(&[
            ("ns", "fleet"),
            ("expires_within", "10m"),
            ("prefix", "nope")
        ])
        .await
        .len(),
        0
    );

    // An `eventual` namespace.
    let seen = select(&[("ns", "shared"), ("expires_within", "10m"), ("limit", "20")]).await;
    assert_eq!(seen.len(), 50);
    assert!(seen.iter().all(|k| k.starts_with("seen:")));
    assert_eq!(
        select(&[
            ("ns", "shared"),
            ("expires_within", "10m"),
            ("prefix", "seen:0")
        ])
        .await
        .len(),
        10
    );
    assert_eq!(
        select(&[("ns", "shared"), ("expires_within", "2d")])
            .await
            .len(),
        100
    );
    assert_eq!(
        select(&[("ns", "shared"), ("no_expiry", "true")])
            .await
            .len(),
        0
    );

    // EMQX's table: no key expires.
    assert_eq!(
        select(&[("ns", "kv_store"), ("no_expiry", "true")])
            .await
            .len(),
        51
    );
    assert_eq!(select(&[("ns", "kv_store")]).await.len(), 51);
    assert_eq!(
        select(&[("ns", "kv_store"), ("expires_within", "30d")])
            .await
            .len(),
        0
    );
    let some = select(&[
        ("ns", "kv_store"),
        ("no_expiry", "true"),
        ("prefix", "dev-1"),
    ])
    .await;
    assert_eq!(some.len(), 10);

    // What contradicts, and what is not what its parameter takes: refused, saying why.
    for (params, why) in [
        (
            &[
                ("ns", "fleet"),
                ("no_expiry", "true"),
                ("expires_within", "1h"),
            ][..],
            "expires_within and no_expiry select nothing together",
        ),
        (
            &[("ns", "fleet"), ("expires_within", "soon")],
            "is not a duration",
        ),
        (
            &[("ns", "fleet"), ("expires_within", "1.5h")],
            "is not a duration",
        ),
        (
            &[("ns", "fleet"), ("no_expiry", "1")],
            "no_expiry is true or false",
        ),
        (
            &[("ns", "fleet"), ("values", "all")],
            "values is true or false",
        ),
    ] {
        let path = with_query("/admin/v1/rules/state/keys", params);
        let (status, body) = n.call(&n.operator, "GET", &path, None).await;
        assert_eq!(
            (status, code(&body)),
            (400, "bad-request"),
            "{path}: {body}"
        );
        let message = body["error"]["message"].as_str().unwrap();
        assert!(message.contains(why), "{path}: {message}");
    }

    // Between two pages of a filtered listing: keys the filter selects are written,
    // and one that was selected is given a later expiry.
    let first = with_query(
        "/admin/v1/rules/state/keys",
        &[("ns", "fleet"), ("expires_within", "10m"), ("limit", "100")],
    );
    let (_, page) = n.call(&n.operator, "GET", &first, None).await;
    assert_eq!(page["count"], 100, "{page}");
    let cursor = page["next_cursor"].as_str().unwrap().to_string();
    let mut seen = names(page["keys"].as_array().unwrap());
    for i in 0..200 {
        let soon = format!(r#"{{"k":"new:{i:03}:soon","v":{i},"ttl":30}}"#);
        n.publish("soon/1", soon.as_bytes());
    }
    let (rest, _) = list_keys(
        &n,
        &n.operator,
        &[
            ("ns", "fleet"),
            ("expires_within", "10m"),
            ("limit", "100"),
            ("cursor", &cursor),
        ],
    )
    .await;
    seen.extend(names(&rest));
    let distinct: std::collections::BTreeSet<&String> = seen.iter().collect();
    assert_eq!(distinct.len(), seen.len(), "no key is listed twice");
    assert!((0..300).all(|i| distinct.contains(&format!("dev:{i:03}:soon"))));
    assert!(seen.iter().all(|k| k.ends_with(":soon")));
    assert!(seen.iter().any(|k| k.starts_with("new:")));
}

/// T12: a page with values ends at 1,000 keys or where its values pass 1 MiB of the
/// answer, whichever comes first. Such a page is short and has a cursor, and the
/// listing is whole all the same; without values the same keys fit one page.
#[tokio::test]
async fn a_page_with_values_ends_at_a_mebibyte_of_them() {
    const BOUND: usize = 1024 * 1024;
    let n = Node::start(EXPIRING).await;
    let value = "v".repeat(4_000);
    for i in 0..400 {
        let payload = format!(r#"{{"k":"big:{i:03}","v":"{value}"}}"#);
        n.publish("later/1", payload.as_bytes());
    }
    let (plain, pages) = list_keys(&n, &n.operator, &[("ns", "fleet"), ("limit", "1000")]).await;
    assert_eq!((plain.len(), pages), (400, 1));
    let mut cursor = None::<String>;
    let (mut keys, mut sizes) = (Vec::new(), Vec::new());
    loop {
        let mut params = vec![("ns", "fleet"), ("limit", "1000"), ("values", "true")];
        if let Some(cursor) = &cursor {
            params.push(("cursor", cursor));
        }
        let path = with_query("/admin/v1/rules/state/keys", &params);
        let (status, body) = n.call(&n.operator, "GET", &path, None).await;
        assert_eq!(status, 200, "{body}");
        let page = body["keys"].as_array().unwrap();
        // What the values take of the answer: each as the JSON string it is there.
        let taken: usize = page.iter().map(|k| k["value"].to_string().len()).sum();
        assert!(taken <= BOUND, "{taken} bytes of values in one page");
        assert!(page.iter().all(|k| k["value"] == value.as_str()));
        sizes.push(taken);
        keys.extend(page.iter().cloned());
        match body["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => break,
        }
        assert!(sizes.len() < 50, "the listing does not end");
    }
    assert_eq!(names(&keys), names(&plain), "every key once");
    // 400 values of 4,002 bytes are 1.6 MiB: two pages, the first as full as it gets.
    assert_eq!(sizes.len(), 2, "{sizes:?}");
    assert!(sizes[0] > BOUND - 4_002, "{sizes:?}");
}

/// T12: the audit log's record of a listing keeps D10's rule. It says which filters
/// were used and whether values were asked for, and names no key: the prefix and the
/// cursor, which holds the last key listed, are recorded by hash and length.
#[tokio::test]
async fn the_record_of_a_filtered_listing_names_its_filters_and_no_key() {
    let n = Node::start(EXPIRING).await;
    for i in 0..30 {
        let soon = format!(r#"{{"k":"secret-device-{i:02}","v":"a secret reading","ttl":60}}"#);
        n.publish("soon/1", soon.as_bytes());
    }
    let (keys, pages) = list_keys(
        &n,
        &n.operator,
        &[
            ("ns", "fleet"),
            ("prefix", "secret-"),
            ("expires_within", "10m"),
            ("values", "true"),
            ("limit", "20"),
        ],
    )
    .await;
    assert_eq!((keys.len(), pages), (30, 2));
    let (keys, _) = list_keys(
        &n,
        &n.operator,
        &[("ns", "kv_store"), ("no_expiry", "true")],
    )
    .await;
    assert!(keys.is_empty());
    let requests: Vec<String> = n
        .audit
        .of_kind("admin.request")
        .into_iter()
        .filter(|(who, _)| who.as_deref() == Some("CN=operator"))
        .map(|(_, detail)| detail)
        .collect();
    let prefix = sha256_hex("secret-");
    assert_eq!(
        requests[0],
        format!(
            "role=operator GET /admin/v1/rules/state/keys?ns=fleet&prefix_sha256={prefix}&\
             prefix_bytes=7&expires_within=10m&values=true&limit=20 -> 200"
        )
    );
    assert!(
        requests[1].starts_with(&format!(
            "role=operator GET /admin/v1/rules/state/keys?ns=fleet&prefix_sha256={prefix}&\
             prefix_bytes=7&expires_within=10m&values=true&limit=20&cursor_sha256="
        )) && requests[1].contains("&cursor_bytes="),
        "{requests:?}"
    );
    assert_eq!(
        requests[2],
        "role=operator GET /admin/v1/rules/state/keys?ns=kv_store&no_expiry=true -> 200"
    );
    // Nothing of a key or a value, as text or as the cursor's hex of it.
    // `secret-device`, as a cursor would carry it.
    let hex = "7365637265742d646576696365";
    for record in &requests {
        assert!(
            !record.contains("secret-") && !record.contains("reading") && !record.contains(hex),
            "{record}"
        );
    }
}
