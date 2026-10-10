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
        let initial = Arc::new(RuleSet::parse(text).unwrap().rules);
        let (rules_tx, rules_rx) = watch::channel(initial);
        let load_instead = Arc::new(Mutex::new(None::<PathBuf>));
        reloader.attach_rules(rules_tx, {
            let (live, instead) = (live.clone(), load_instead.clone());
            move || {
                let path = match instead.lock().unwrap().clone() {
                    Some(other) => Some(other.display().to_string()),
                    None => live.read().unwrap().rules.file.clone(),
                };
                mqttd::rules::load(path.as_deref()).map(Arc::new)
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
            .with_observe(observe.clone());

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
