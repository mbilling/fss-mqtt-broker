//! State for rules (ADR 0085), the rule engine's half: how a file declares and names
//! namespaces, what a state function hands the store, when a call reaches it, and what
//! the engine itself bounds. The store here is a test double that records every call;
//! the broker's store, and EMQX's probed results for `kv_store_*`, are tested in
//! `mqttd` (`rule_state`).

use std::collections::HashMap;
use std::sync::Mutex;

use super::*;
use crate::state::{
    term_key, written_len, NamespaceSettings, StateError, StateNamespace, StateOp, StateReply,
    StateStore,
};

/// What the recording store holds: values by (namespace, key).
type Data = Arc<Mutex<HashMap<(String, Vec<u8>), Value>>>;

/// A store that keeps what it is told in plain maps and writes down every call, as
/// `<namespace> <op> <key>`.
#[derive(Debug, Default)]
pub(crate) struct Recorder {
    log: Arc<Mutex<Vec<String>>>,
    data: Data,
    opened: Mutex<Vec<(String, NamespaceSettings)>>,
    /// What a `Get` says its answer weighs.
    read_bytes: usize,
}

#[derive(Debug)]
struct Recorded {
    name: String,
    log: Arc<Mutex<Vec<String>>>,
    data: Data,
    read_bytes: usize,
}

impl StateStore for Recorder {
    fn max_bytes(&self) -> u64 {
        1 << 20
    }

    fn open(
        &self,
        name: &str,
        settings: &NamespaceSettings,
    ) -> Result<Arc<dyn StateNamespace>, String> {
        self.opened
            .lock()
            .unwrap()
            .push((name.to_string(), settings.clone()));
        Ok(Arc::new(Recorded {
            name: name.to_string(),
            log: self.log.clone(),
            data: self.data.clone(),
            read_bytes: self.read_bytes,
        }))
    }
}

impl StateNamespace for Recorded {
    fn call(&self, op: &StateOp<'_>) -> Result<StateReply, StateError> {
        let (what, key) = match op {
            StateOp::Get { key } => ("get", key),
            StateOp::Set { key, .. } => ("set", key),
            StateOp::SetOnce { key, .. } => ("set_once", key),
            StateOp::Incr { key, .. } => ("incr", key),
            StateOp::IncrOnce { key, .. } => ("incr_once", key),
            StateOp::Extreme { key, .. } => ("extreme", key),
            StateOp::Add { key, .. } => ("add", key),
            StateOp::Del { key } => ("del", key),
            StateOp::Ttl { key } => ("ttl", key),
            StateOp::Held { key, .. } => ("held", key),
            StateOp::Changed { key, .. } => ("changed", key),
            StateOp::HllAdd { key, .. } => ("hll_add", key),
            StateOp::HllCount { key } => ("hll_count", key),
        };
        let mut line = format!("{} {what} {}", self.name, String::from_utf8_lossy(key));
        if let StateOp::IncrOnce { once, .. } = op {
            line.push_str(" once ");
            line.push_str(&String::from_utf8_lossy(once));
        }
        self.log.lock().unwrap().push(line);
        let at = (self.name.clone(), key.to_vec());
        let mut data = self.data.lock().unwrap();
        let value = match op {
            StateOp::Get { .. } => {
                return Ok(StateReply {
                    value: data.get(&at).cloned(),
                    read_bytes: self.read_bytes,
                })
            }
            StateOp::Set { value, .. } => {
                data.insert(at, value.value.clone());
                None
            }
            StateOp::Incr { by, .. } => {
                let after = match data.get(&at) {
                    Some(Value::Int(n)) => n + by,
                    _ => *by,
                };
                data.insert(at, Value::Int(after));
                Some(Value::Int(after))
            }
            StateOp::Del { .. } => Some(Value::Bool(data.remove(&at).is_some())),
            _ => None,
        };
        Ok(StateReply {
            value,
            read_bytes: 0,
        })
    }
}

pub(crate) fn with_store(store: &Arc<Recorder>) -> Functions {
    Functions::default().with_state(store.clone())
}

fn log_of(store: &Recorder) -> Vec<String> {
    std::mem::take(&mut *store.log.lock().unwrap())
}

/// One statement on one message with a `payload`, on a fresh recording store: its
/// outputs (or its error) and the calls that reached the store, in order. A lone
/// statement's namespace is `rule:test`; EMQX's table is `kv_store`.
fn run(sql: &str, payload: &str) -> (Result<Vec<String>, String>, Vec<String>) {
    let store = Arc::new(Recorder::default());
    let payload = Bytes::from(payload.to_string());
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
    let out = test_sql_with(sql, &input, &with_store(&store));
    (out, log_of(&store))
}

fn calls(sql: &str, payload: &str) -> Vec<String> {
    run(sql, payload).1
}

fn error(sql: &str, payload: &str) -> String {
    run(sql, payload).0.expect_err(sql)
}

fn load(text: &str, store: &Arc<Recorder>) -> Result<Loaded, String> {
    RuleSet::parse_with(text, &with_store(store)).map_err(|e| e.to_string())
}

fn load_error(text: &str) -> String {
    load(text, &Arc::new(Recorder::default())).expect_err(text)
}

/// Every message the rules of `set` select, evaluated as the broker evaluates it.
fn publish(set: &RuleSet, topic: &str, payload: &str) -> Vec<String> {
    let payload = Bytes::from(payload.to_string());
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", topic, &payload, 1, &props);
    let mut results = Vec::new();
    let mut out = Vec::new();
    set.on_publish(
        &input,
        &mut |rule, outcome| match outcome {
            Outcome::Passed => results.push(format!("{} passed", rule.id())),
            Outcome::NoResult => results.push(format!("{} no_result", rule.id())),
            Outcome::Failed(e) => results.push(format!("{} failed: {e}", rule.id())),
            _ => {}
        },
        &mut out,
    );
    results
}

// -- loading ---------------------------------------------------------------------------

/// D2: loaded with no store, a file that calls a state function is refused, as a file
/// that calls `jq` is where `jq` is off; switched off, the error names the key.
#[test]
fn a_file_that_uses_state_is_refused_where_there_is_no_store() {
    let no_store = "rule state is not available here: the rules are loaded without a state store";
    for call in ["kv_set('k', 1)", "kv_get('k')", "kv_store_put('k', 1)"] {
        let text = format!("[rules.r]\nsql = '''SELECT {call} AS v FROM \"t/#\"'''\n");
        let e = RuleSet::parse(&text).expect_err(call).to_string();
        assert!(e.starts_with("rule `r`: ") && e.contains(no_store), "{e}");
        let sql = format!("SELECT {call} AS v FROM \"t/#\"");
        assert!(check_sql(&sql).expect_err(call).contains(no_store));
        let off = RuleSet::parse_with(&text, &Functions::default().without_state())
            .expect_err(call)
            .to_string();
        assert!(
            off.contains(
                "rule state is switched off: `rules.state_max_bytes` is 0 (set it above 0 for \
                 rules to keep state; see RULES.md)"
            ),
            "{off}"
        );
    }
    // Declaring a namespace is using state, whether or not a rule calls it.
    let declared = "[state.alerts]\n[rules.r]\nsql = '''SELECT 1 AS v FROM \"t/#\"'''\n";
    assert_eq!(
        RuleSet::parse(declared).unwrap_err().to_string(),
        format!("rules file: [state.alerts]: {no_store}")
    );
    assert!(
        RuleSet::parse_with(declared, &Functions::default().without_state())
            .unwrap_err()
            .to_string()
            .contains("`rules.state_max_bytes` is 0")
    );
    // A file that uses none loads anywhere, and holds no namespace.
    let plain = RuleSet::parse("[rules.r]\nsql = '''SELECT 1 AS v FROM \"t/#\"'''\n").unwrap();
    assert_eq!(plain.rules.state_namespaces().count(), 0);
}

/// D1: a rule uses the namespace its `state` key names, or one of its own; EMQX's
/// functions use EMQX's table whatever the key says; and the namespace is never read
/// out of a key.
#[test]
fn a_rule_names_its_namespace_and_a_key_never_selects_one() {
    let store = Arc::new(Recorder::default());
    let text = r#"
[state.alerts]
[state.unused]

[rules.a]
state = "alerts"
sql = '''SELECT kv_set('alerts:x', 1) AS v, kv_store_put('p', 1) AS p FROM "t/#"'''

[rules.b]
state = "alerts"
sql = '''SELECT kv_get('alerts:x') AS v FROM "t/#"'''

[rules.c]
sql = '''SELECT kv_set('alerts:x', 2) AS v, kv_store_get('p') AS p FROM "t/#"'''
"#;
    let loaded = load(text, &store).unwrap();
    assert_eq!(
        publish(&loaded.rules, "t/1", "{}"),
        ["a passed", "b passed", "c passed"]
    );
    assert_eq!(
        log_of(&store),
        [
            "alerts set alerts:x",
            "kv_store set \u{7}\0\0\0\0\0\0\0\u{1}p",
            "alerts get alerts:x",
            "rule:c set alerts:x",
            "kv_store get \u{7}\0\0\0\0\0\0\0\u{1}p",
        ]
    );
    // Every namespace the file holds, the declared-but-unused one included.
    let names: Vec<&str> = loaded.rules.state_namespaces().map(|(n, _)| n).collect();
    assert_eq!(names, ["alerts", "kv_store", "rule:c", "unused"]);
    assert_eq!(loaded.rules.get("a").unwrap().state(), Some("alerts"));
    assert_eq!(loaded.rules.get("c").unwrap().state(), None);
    // One namespace is opened once per file, however many rules and calls use it.
    let mut opened: Vec<String> = store
        .opened
        .lock()
        .unwrap()
        .iter()
        .map(|(n, _)| n.clone())
        .collect();
    opened.sort();
    assert_eq!(opened, ["alerts", "kv_store", "rule:c", "unused"]);
    assert_eq!(
        loaded.warnings,
        [
            "the file calls both EMQX's kv_store_* functions and mqttd's kv_* functions: they do \
          not share keys (kv_get does not read what kv_store_put wrote)",
            crate::STATE_EXPERIMENTAL_WARNING,
        ]
    );
}

#[test]
fn a_state_key_must_name_a_declared_namespace() {
    let rule = |state: &str| {
        format!("[state.alerts]\n[rules.r]\nstate = \"{state}\"\nsql = '''SELECT 1 AS v FROM \"t/#\"'''\n")
    };
    assert!(load(&rule("alerts"), &Arc::new(Recorder::default())).is_ok());
    assert_eq!(
        load_error(&rule("nope")),
        "rule `r`: state = \"nope\": the file declares no [state.nope]"
    );
    for reserved in ["default", "kv_store"] {
        let e = load_error(&rule(reserved));
        assert!(
            e.starts_with(&format!(
                "rule `r`: state = \"{reserved}\": `{reserved}` is reserved"
            )),
            "{e}"
        );
    }
}

/// D1, D9: a declaration's names, modes and bounds.
#[test]
fn a_namespace_declaration_is_checked_when_the_file_loads() {
    let declared = |name: &str, body: &str| {
        load_error(&format!(
            "[state.{name}]\n{body}\n[rules.r]\nsql = '''SELECT 1 AS v FROM \"t/#\"'''\n"
        ))
    };
    for (name, body, wanted) in [
        ("Alerts", "", "a namespace's name is a lower-case letter followed by up to 63"),
        ("a-b", "", "a namespace's name is a lower-case letter followed by up to 63"),
        ("a", "consistency = \"strong\"", "consistency = \"strong\" is not available: it is a later phase (ADR 0085 D5); use \"local\" or \"eventual\""),
        ("a", "consistency = \"quorum\"", "consistency = \"quorum\": expected \"local\" or \"eventual\""),
        ("a", "consistency = \"eventual\"\neviction = \"lru\"", "eviction = \"lru\" is for a \"local\" namespace"),
        ("a", "tombstone_window = \"1h\"", "tombstone_window is for an \"eventual\" namespace"),
        ("a", "consistency = \"eventual\"\ntombstone_window = 0", "tombstone_window = 0: expected 1 second to 30 days"),
        ("a", "consistency = \"eventual\"\ntombstone_window = \"later\"", "tombstone_window: \"later\" is not a duration"),
        ("a", "max_keys = 0", "max_keys = 0: expected 1 to 1000000"),
        ("a", "max_keys = 1000001", "max_keys = 1000001: expected 1 to 1000000"),
        ("a", "max_bytes = \"2MiB\"", "max_bytes = \"2MiB\": expected 1 byte to the store's ceiling, 1048576 bytes (`rules.state_max_bytes`)"),
        ("a", "max_bytes = 0", "max_bytes = 0: expected 1 byte to the store's ceiling"),
        ("a", "max_bytes = \"lots\"", "max_bytes: \"lots\" is not a size"),
        ("a", "max_ttl = \"31d\"", "max_ttl = \"31d\": expected 1 second to 30 days (2592000 seconds)"),
        ("a", "default_ttl = 0", "default_ttl is 0 seconds: expected 1 second to max_ttl (2592000 seconds)"),
        ("a", "max_ttl = \"1h\"", "default_ttl is 86400 seconds: expected 1 second to max_ttl (3600 seconds)"),
        ("a", "default_ttl = \"soon\"", "default_ttl: \"soon\" is not a duration"),
        ("a", "eviction = \"random\"", "eviction = \"random\": expected \"fail\" or \"lru\""),
        ("a", "ttl = 5", "unknown field `ttl`"),
        ("kv_store", "default_ttl = \"1h\"", "EMQX's kv_store table has no expiry: default_ttl and max_ttl cannot be set"),
        ("kv_store", "consistency = \"eventual\"", "EMQX's kv_store table is one per node; it is always \"local\""),
        ("a", "dedupe_window = 0", "dedupe_window = 0: expected 1 second to 1 hour (3600 seconds), and at most max_ttl (2592000 seconds)"),
        ("a", "dedupe_window = \"2h\"", "dedupe_window = \"2h\": expected 1 second to 1 hour (3600 seconds)"),
        ("a", "max_ttl = \"10m\"\ndefault_ttl = \"1m\"\ndedupe_window = \"20m\"", "dedupe_window = \"20m\": expected 1 second to 1 hour (3600 seconds), and at most max_ttl (600 seconds)"),
        ("a", "dedupe_window = \"soon\"", "dedupe_window: \"soon\" is not a duration"),
        ("kv_store", "dedupe_window = \"1m\"", "dedupe_window is for the namespaces kv_incr counts in"),
        ("default", "max_keys = 0", "[state.default]: max_keys = 0"),
    ] {
        let e = declared(name, body);
        assert!(e.contains(wanted), "[state.{name}] {body}: {e}");
    }
    // What a table may say, as the settings the store is given.
    let store = Arc::new(Recorder::default());
    let text = "[state.cache]\nmax_keys = 10\nmax_bytes = \"64KiB\"\ndefault_ttl = \"10m\"\n\
                max_ttl = \"1h\"\neviction = \"lru\"\n\
                [state.default]\nmax_keys = 5\n[state.kv_store]\nmax_keys = 7\n\
                [rules.r]\nsql = '''SELECT kv_get('k') AS v, kv_store_get('k') AS w FROM \"t/#\"'''\n";
    load(text, &store).unwrap();
    let opened = store.opened.lock().unwrap().clone();
    let settings = |name: &str| opened.iter().find(|(n, _)| n == name).unwrap().1.clone();
    assert_eq!(
        settings("cache"),
        NamespaceSettings {
            consistency: state::Consistency::Local,
            max_keys: 10,
            max_bytes: 64 << 10,
            default_ttl_secs: Some(600),
            max_ttl_secs: 3600,
            eviction: state::Eviction::Lru,
            tombstone_window_secs: 3600,
            dedupe_window_secs: 300,
        }
    );
    // A rule's own namespace gets `[state.default]`; the store's ceiling (1 MiB here)
    // caps the default `max_bytes`.
    assert_eq!(
        settings("rule:r"),
        NamespaceSettings {
            max_keys: 5,
            ..NamespaceSettings::defaults(1 << 20)
        }
    );
    assert_eq!(settings("rule:r").max_bytes, 1 << 20);
    assert_eq!(
        settings("kv_store"),
        NamespaceSettings {
            max_keys: 7,
            default_ttl_secs: None,
            ..NamespaceSettings::defaults(1 << 20)
        }
    );
    // D5: an `eventual` namespace, and how long it remembers a delete.
    let store = Arc::new(Recorder::default());
    let text = "[state.shared]\nconsistency = \"eventual\"\ntombstone_window = \"10m\"\n\
                [state.plain]\nconsistency = \"eventual\"\n\
                [rules.r]\nsql = '''SELECT 1 AS v FROM \"t/#\"'''\n";
    load(text, &store).unwrap();
    let opened = store.opened.lock().unwrap().clone();
    let settings = |name: &str| opened.iter().find(|(n, _)| n == name).unwrap().1.clone();
    assert_eq!(
        settings("shared"),
        NamespaceSettings {
            consistency: state::Consistency::Eventual,
            tombstone_window_secs: 600,
            ..NamespaceSettings::defaults(1 << 20)
        }
    );
    assert_eq!(settings("plain").tombstone_window_secs, 3600);
}

/// D8: how long a namespace remembers an idempotency key. Five minutes unless the
/// table says, and never longer than a key of the namespace may live.
#[test]
fn a_namespace_declares_how_long_it_remembers_an_idempotency_key() {
    let store = Arc::new(Recorder::default());
    let text = "[state.plain]\n[state.long]\ndedupe_window = \"1h\"\n\
                [state.brief]\nmax_ttl = 60\ndefault_ttl = 30\n\
                [rules.r]\nsql = '''SELECT 1 AS v FROM \"t/#\"'''\n";
    load(text, &store).unwrap();
    let opened = store.opened.lock().unwrap().clone();
    let window = |name: &str| {
        let settings = &opened.iter().find(|(n, _)| n == name).unwrap().1;
        settings.dedupe_window_secs
    };
    assert_eq!(
        (window("plain"), window("long"), window("brief")),
        (300, 3600, 60)
    );
}

#[test]
fn a_file_declares_at_most_64_namespaces() {
    let file = |n: usize| {
        let mut text = String::from("[state.default]\n[state.kv_store]\n");
        for i in 0..n {
            text.push_str("[state.n");
            text.push_str(&i.to_string());
            text.push_str("]\n");
        }
        text.push_str("[rules.r]\nsql = '''SELECT 1 AS v FROM \"t/#\"'''\n");
        text
    };
    assert!(load(&file(64), &Arc::new(Recorder::default())).is_ok());
    assert_eq!(
        load_error(&file(65)),
        "rules file: 65 [state.<name>] tables is more than the 64 a file may declare"
    );
}

/// The state functions are built in: an unknown arity is refused at load like any
/// function's, and a module can no longer take one of the names.
#[test]
fn the_state_functions_are_built_in() {
    for name in [
        "kv_store_put",
        "kv_store_get",
        "kv_store_del",
        "kv_get",
        "kv_set",
        "kv_set_once",
        "kv_incr",
        "kv_max",
        "kv_min",
        "kv_add",
        "kv_del",
        "kv_ttl",
        "kv_held",
        "kv_changed",
        "kv_hll_add",
        "kv_hll_count",
    ] {
        assert!(function_names().contains(&name), "{name}");
        assert_eq!(
            reserved_function_name(name),
            Some("a built-in function has that name"),
            "{name}"
        );
        assert!(!emqx_only_function_names().contains(&name), "{name}");
    }
    for (call, takes) in [
        (
            "kv_store_put('a')",
            "kv_store_put() takes 2 argument(s), 1 given",
        ),
        (
            "kv_store_put('a', 1, 2)",
            "kv_store_put() takes 2 argument(s), 3 given",
        ),
        (
            "kv_store_get()",
            "kv_store_get() takes 1..=2 argument(s), 0 given",
        ),
        (
            "kv_store_get('a', 1, 2)",
            "kv_store_get() takes 1..=2 argument(s), 3 given",
        ),
        (
            "kv_store_del('a', 1)",
            "kv_store_del() takes 1 argument(s), 2 given",
        ),
        (
            "kv_incr('a', 1, 60, 'idem', 2)",
            "kv_incr() takes 1..=4 argument(s), 5 given",
        ),
        (
            "kv_hll_add('a')",
            "kv_hll_add() takes 2..=3 argument(s), 1 given",
        ),
        (
            "kv_hll_count('a', 1)",
            "kv_hll_count() takes 1 argument(s), 2 given",
        ),
    ] {
        let e = error(&format!("SELECT {call} AS v FROM \"t/#\""), "{}");
        assert!(e.contains(takes), "{call}: {e}");
    }
}

// -- arguments -------------------------------------------------------------------------

/// D3: a key is a string of 1 to 256 bytes, a value anything but `undefined` of at most
/// 4 KiB as written, a TTL whole seconds or a duration within `max_ttl`. Anything else
/// fails the rule, and nothing reaches the store.
#[test]
fn a_call_with_a_bad_key_value_or_ttl_fails_the_rule_before_the_store() {
    let long_key = "k".repeat(257);
    let big = "v".repeat(4097);
    let cases: Vec<(String, &str)> = vec![
        ("kv_get(1)".into(), "kv_get(): bad_key: a key is a string, got a integer"),
        ("kv_get('')".into(), "kv_get(): bad_key: a key is 1 to 256 bytes, got 0"),
        (format!("kv_set('{long_key}', 1)"), "kv_set(): bad_key: a key is 1 to 256 bytes, got 257"),
        ("kv_get(payload.nope)".into(), "kv_get(): bad_key: a key is a string, got a undefined"),
        ("kv_set('k', payload.nope)".into(), "kv_set(): bad_value: a value cannot be undefined (kv_del deletes a key)"),
        (format!("kv_set('k', '{big}')"), "kv_set(): value_too_large: a value is 4097 bytes as written; at most 4096"),
        (format!("kv_changed('k', '{big}')"), "kv_changed(): value_too_large: a value is 4097 bytes as written; at most 4096"),
        (format!("kv_set_once('k', '{big}')"), "kv_set_once(): value_too_large:"),
        (format!("kv_add('k', '{long_key}')"), "kv_add(): value_too_large: an element is 257 bytes as written; at most 256"),
        ("kv_add('k', payload.nope)".into(), "kv_add(): bad_value: an element cannot be undefined"),
        ("kv_set('k', 1, 0)".into(), "kv_set(): bad_ttl: a TTL is a positive integer of seconds or a duration such as '10m', at least one second"),
        ("kv_set('k', 1, -5)".into(), "kv_set(): bad_ttl: a TTL is a positive integer"),
        ("kv_set('k', 1, 1.5)".into(), "kv_set(): bad_ttl: a TTL is a positive integer"),
        ("kv_set('k', 1, 'soon')".into(), "kv_set(): bad_ttl: a TTL is a positive integer"),
        ("kv_set('k', 1, '0s')".into(), "kv_set(): bad_ttl: a TTL is a positive integer"),
        ("kv_set('k', 1, 2592001)".into(), "kv_set(): bad_ttl: a TTL is at most the namespace's max_ttl, 2592000 seconds"),
        ("kv_set('k', 1, '31d')".into(), "kv_set(): bad_ttl: a TTL is at most the namespace's max_ttl, 2592000 seconds"),
        ("kv_held('k', true, 0)".into(), "kv_held(): bad_ttl:"),
        ("kv_incr('k', 1.5)".into(), "kv_incr(): bad_value: kv_incr adds a 64-bit integer, got a float"),
        ("kv_incr('k', 'x')".into(), "kv_incr(): bad_value: kv_incr adds a 64-bit integer, got a string"),
        ("kv_incr('k', 18446744073709551616)".into(), "kv_incr(): bad_value: kv_incr adds a 64-bit integer, got a integer"),
        ("kv_max('k', 'x')".into(), "kv_max(): bad_value: expected a number, got a string"),
        ("kv_min('k', true)".into(), "kv_min(): bad_value: expected a number, got a boolean"),
        (format!("kv_store_put('{long_key}', 1)"), "kv_store_put(): bad_key: a key is 257 bytes as written; at most 256"),
        (format!("kv_store_get('{long_key}')"), "kv_store_get(): bad_key:"),
        (format!("kv_store_put('k', '{big}')"), "kv_store_put(): value_too_large: a value is 4097 bytes as written; at most 4096"),
    ];
    for (call, wanted) in cases {
        let (out, log) = run(&format!("SELECT {call} AS v FROM \"t/#\""), "{}");
        let e = out.expect_err(&call);
        assert!(e.contains(wanted), "{call}: {e}");
        assert!(log.is_empty(), "{call}: {log:?}");
    }
    // At the bounds, and with each form of TTL, the call reaches the store.
    let key = "k".repeat(256);
    let value = "v".repeat(4096);
    for call in [
        format!("kv_set('{key}', '{value}')"),
        "kv_set('k', 1, 1)".to_string(),
        "kv_set('k', 1, 2592000)".to_string(),
        "kv_set('k', 1, '10m')".to_string(),
        "kv_set('k', 1, '30d')".to_string(),
        "kv_incr('k', -3, '1h')".to_string(),
        "kv_held('k', payload.nope)".to_string(),
        format!("kv_store_put('{key}', '{value}')"),
        // `undefined` is a key and a value in EMQX's table.
        "kv_store_put(payload.nope, payload.nope)".to_string(),
    ] {
        let (out, log) = run(&format!("SELECT {call} AS v FROM \"t/#\""), "{}");
        assert!(out.is_ok(), "{call}: {out:?}");
        assert_eq!(log.len(), 1, "{call}");
    }
}

/// D8: `kv_incr`'s fourth argument is the idempotency key, a string of 1 to 255 bytes,
/// handed to the store beside the counter's key. A message that has none fails its
/// rule: it is neither counted twice without a word nor left out without one.
#[test]
fn kv_incr_hands_the_store_its_idempotency_key() {
    let sql = "SELECT kv_incr('n', 1, 60, payload.id) AS v FROM \"t/#\"";
    assert_eq!(
        calls(sql, r#"{"id":"m-1"}"#),
        ["rule:test incr_once n once m-1"]
    );
    let longest = "i".repeat(state::MAX_IDEMPOTENCY_KEY_BYTES);
    assert_eq!(
        calls(sql, &format!(r#"{{"id":"{longest}"}}"#)),
        [format!("rule:test incr_once n once {longest}")]
    );
    // Without one it is the call it always was.
    assert_eq!(
        calls("SELECT kv_incr('n', 1, 60) AS v FROM \"t/#\"", "{}"),
        ["rule:test incr n"]
    );
    for (payload, wanted) in [
        ("{}", "an idempotency key is a string, got a undefined"),
        (
            r#"{"id":7}"#,
            "an idempotency key is a string, got a integer",
        ),
        (
            r#"{"id":""}"#,
            "an idempotency key is 1 to 255 bytes, got 0",
        ),
        (
            &format!(r#"{{"id":"{longest}i"}}"#),
            "an idempotency key is 1 to 255 bytes, got 256",
        ),
    ] {
        let (out, log) = run(sql, payload);
        let e = out.expect_err(payload);
        assert_eq!(e, format!("kv_incr(): bad_idempotency_key: {wanted}"));
        assert!(log.is_empty(), "{payload}: {log:?}");
    }
}

/// D8: the namespace remembers an idempotency key under a key of its own that starts
/// with a NUL byte, and no `kv_*` key may: a value a publisher chooses can then
/// neither read nor write what the namespace remembers, and an idempotency key can
/// never name a key of the rule's. EMQX's table matches any term, and is not touched.
#[test]
fn a_key_cannot_start_with_a_nul_byte() {
    let payload = r#"{"k":"\u0000m-1"}"#;
    for call in [
        "kv_get(payload.k)",
        "kv_set(payload.k, 1)",
        "kv_set_once(payload.k, 1)",
        "kv_incr(payload.k)",
        "kv_incr(payload.k, 1, 60, 'm-1')",
        "kv_del(payload.k)",
        "kv_ttl(payload.k)",
        "kv_held(payload.k, true)",
        "kv_changed(payload.k, 1)",
        "kv_add(payload.k, 1)",
        "kv_max(payload.k, 1)",
        "kv_hll_add(payload.k, 1)",
        "kv_hll_count(payload.k)",
    ] {
        let (out, log) = run(&format!("SELECT {call} AS v FROM \"t/#\""), payload);
        let e = out.expect_err(call);
        assert!(
            e.contains("bad_key: a key cannot start with a NUL byte"),
            "{call}: {e}"
        );
        assert!(log.is_empty(), "{call}: {log:?}");
    }
    let (out, log) = run(
        "SELECT kv_store_put(payload.k, 1) AS v FROM \"t/#\"",
        payload,
    );
    assert!(out.is_ok() && log.len() == 1, "{out:?} {log:?}");
    // The entry's key is that byte and then the idempotency key; nothing else is one.
    let entry = state::Remembered::of(b"m-1");
    assert_eq!(entry.key(), b"\0m-1");
    assert_eq!(state::remembered(entry.key()), Some(&b"m-1"[..]));
    assert_eq!(state::remembered(b"m-1"), None);
    // EMQX's table keeps `undefined` under that byte alone.
    assert_eq!(term_key(&Value::Undefined), [state::REMEMBERED_PREFIX]);
    assert_eq!(state::remembered(&term_key(&Value::Undefined)), None);
    assert_eq!(
        state::Remembered::of(&[b'i'; 255]).key().len(),
        state::MAX_KEY_BYTES
    );
}

/// What `kv_store_*` answers is EMQX's whatever the table says (probed on 6.3.1:
/// `put` and `del` are `true`, `get` of a missing key `undefined` or the default).
#[test]
fn the_emqx_functions_answer_as_emqx_does() {
    let (out, _) = run(
        "SELECT kv_store_put('a', 1) AS p, kv_store_get('a') AS g, kv_store_get('nope') AS m, \
         kv_store_get('nope', 'dflt') AS d, kv_store_del('a') AS x, kv_store_del('a') AS y, \
         kv_store_get('a') AS z FROM \"t/#\"",
        "{}",
    );
    // EMQX: #{p => true, g => 1, m => undefined, d => <<"dflt">>, x => true, y => true,
    // z => undefined}; the atom `undefined` is written as its name, as everywhere.
    assert_eq!(
        out.unwrap(),
        [r#"{"p":true,"g":1,"m":"undefined","d":"dflt","x":true,"y":true,"z":"undefined"}"#]
    );
    // `kv_set` answers the value written.
    let (out, _) = run(
        "SELECT kv_set('k', payload.v) AS s FROM \"t/#\"",
        r#"{"v":[1,2.5]}"#,
    );
    assert_eq!(out.unwrap(), [r#"{"s":[1,2.5]}"#]);
}

/// A `kv_store_*` key is matched exactly, as an `ets` `set` table matches (`=:=`):
/// probed on EMQX 6.3.1 with `1`, `1.0` and `<<"1">>`, `0.0` and `-0.0`, atoms against
/// their names, nested numbers, and maps written in another order.
#[test]
fn a_kv_store_key_is_the_exact_term() {
    let k = |text: &str| term_key(&json_decode(text.as_bytes()).unwrap());
    let distinct = [
        "1",
        "1.0",
        "\"1\"",
        "0",
        "0.0",
        "-0.0",
        "true",
        "\"true\"",
        "null",
        "\"null\"",
        "\"\"",
        "[1,2]",
        "[1.0,2.0]",
        "{\"a\":1}",
        "{\"a\":1.0}",
        "[]",
        "{}",
        "18446744073709551616",
        "18446744073709551616.0",
    ];
    for (i, a) in distinct.iter().enumerate() {
        for b in &distinct[i + 1..] {
            assert_ne!(k(a), k(b), "{a} and {b} are two keys");
        }
    }
    assert_ne!(term_key(&Value::Undefined), term_key(&Value::Null));
    assert_ne!(
        term_key(&Value::Undefined),
        term_key(&Value::from("undefined"))
    );
    // One key: a map whatever order it was written in; text and the same bytes.
    assert_eq!(k(r#"{"x":1,"y":2}"#), k(r#"{"y":2,"x":1}"#));
    assert_eq!(
        term_key(&Value::from("ab")),
        term_key(&Value::Bin(Bytes::from_static(b"ab")))
    );
    assert_eq!(k("1"), term_key(&Value::Int(1)));
}

/// ADR 0085 D10: a `kv_store_*` key is shown to an operator as the term it was made
/// of. Every term's key reads back as a term with that very key; bytes that are no
/// term's key, a key cut short and a key with something after it read back as nothing.
#[test]
fn a_kv_store_key_reads_back_as_its_term() {
    use crate::state::key_term;
    let mut terms: Vec<Value> = [
        "1",
        "-1",
        "1.0",
        "0.0",
        "9223372036854775807",
        "-9223372036854775808",
        "9223372036854775808",
        "-123456789012345678901234567890",
        "true",
        "false",
        "null",
        "\"\"",
        "\"text \\u001b with a control character\"",
        "[]",
        "[1,[2.5,[\"x\",null]],{}]",
        "{}",
        r#"{"b":1,"a":{"z":[1,2],"y":"v"}}"#,
    ]
    .iter()
    .map(|text| json_decode(text.as_bytes()).unwrap())
    .collect();
    terms.push(Value::Undefined);
    terms.push(Value::Float(-0.0));
    terms.push(Value::Bin(Bytes::from_static(b"\xff\x00\xfe")));
    terms.push(Value::from(vec![
        Value::Undefined,
        Value::Bin(Bytes::from_static(b"\xff")),
    ]));
    for term in &terms {
        let key = term_key(term);
        let back = key_term(&key).unwrap_or_else(|| panic!("{term:?}"));
        assert_eq!(term_key(&back), key, "{term:?} came back as {back:?}");
        assert_eq!(back.type_name(), term.type_name(), "{term:?}");
        // Cut short anywhere, or with a byte after it, it is no key.
        for cut in 0..key.len() {
            assert!(key_term(&key[..cut]).is_none(), "{term:?} cut at {cut}");
        }
        let mut longer = key.clone();
        longer.push(0);
        assert!(key_term(&longer).is_none(), "{term:?}");
    }
    // Text comes back as text, and bytes that are not text as a binary.
    assert!(matches!(
        key_term(&term_key(&Value::Bin(Bytes::from_static(b"ab")))),
        Some(Value::Str(_))
    ));
    // An unknown tag; a length the bytes do not hold; nesting without end.
    let mut huge = vec![8];
    huge.extend_from_slice(&u64::MAX.to_be_bytes());
    let mut deep = Vec::new();
    for _ in 0..10_000 {
        deep.push(8);
        deep.extend_from_slice(&1u64.to_be_bytes());
    }
    deep.push(1);
    for bytes in [vec![10], vec![255], huge, deep] {
        assert!(
            key_term(&bytes).is_none(),
            "{:?}",
            &bytes[..bytes.len().min(12)]
        );
    }
}

#[test]
fn a_value_is_measured_as_the_rule_engine_writes_it() {
    for (text, bytes) in [
        ("\"abc\"", 3),
        ("\"\"", 0),
        ("0", 1),
        ("-12", 3),
        ("9223372036854775807", 19),
        ("true", 4),
        ("false", 5),
        ("null", 4),
        ("1.5", 3),
        ("[1,2.5,\"s\"]", 11),
        ("{\"a\":1}", 7),
        ("123456789012345678901234567890", 30),
    ] {
        let v = json_decode(text.as_bytes()).unwrap();
        assert_eq!(written_len(&v), bytes, "{text}");
    }
    assert_eq!(written_len(&Value::Int(i64::MIN)), 20);
    assert_eq!(
        written_len(&Value::Bin(Bytes::from_static(&[0xff, 0xfe]))),
        2
    );
}

// -- when a call runs ------------------------------------------------------------------

/// D3: the sketch functions are state calls like the others. What reaches the store is
/// the value's hash, by its exact identity: the value itself is never kept.
#[test]
fn the_sketch_functions_check_their_arguments_and_hand_the_store_a_hash() {
    assert_eq!(
        calls(
            "SELECT kv_hll_add('seen', clientid, '1h') AS n, kv_hll_count('seen') AS c FROM \"t/#\"",
            "{}"
        ),
        ["rule:test hll_add seen", "rule:test hll_count seen"]
    );
    for (call, reason) in [
        ("kv_hll_add('k', payload.missing)", "bad_value"),
        ("kv_hll_add(1, 'v')", "bad_key"),
        ("kv_hll_add('k', 'v', 0)", "bad_ttl"),
        ("kv_hll_count('')", "bad_key"),
    ] {
        let (out, log) = run(&format!("SELECT {call} AS v FROM \"t/#\""), "{}");
        let e = out.expect_err(call);
        assert!(e.contains(reason), "{call}: {e}");
        assert!(log.is_empty(), "{call}: {log:?}");
    }
    // `1` and `1.0` are two values, text and the same bytes one, and a map is the same
    // value whatever order its keys are in.
    let hash =
        |text: &str| crate::crdt::stable_hash(&term_key(&json_decode(text.as_bytes()).unwrap()));
    assert_ne!(hash("1"), hash("1.0"));
    assert_ne!(hash("1"), hash("\"1\""));
    assert_eq!(hash("{\"a\":1,\"b\":2}"), hash("{\"b\":2,\"a\":1}"));
    assert_eq!(
        crate::crdt::stable_hash(&term_key(&Value::from("a"))),
        crate::crdt::stable_hash(&term_key(&Value::Bin(Bytes::from_static(b"a"))))
    );
}

/// D3, EMQX's order (`emqx_rule_runtime:evaluate_select/3`): every `SELECT` field left
/// to right, then the `WHERE`. A call in a field reaches the store for a message the
/// `WHERE` turns away.
#[test]
fn a_call_in_a_select_field_runs_for_a_message_the_where_rejects() {
    let (out, log) = run(
        "SELECT kv_store_put('w', 7) AS p, kv_incr('n') AS n FROM \"t/#\" WHERE 1 = 2",
        "{}",
    );
    assert_eq!(out.unwrap(), Vec::<String>::new());
    assert_eq!(log.len(), 2, "{log:?}");
    assert_eq!(log[1], "rule:test incr n");
    // The fields run in the order written, and the WHERE after all of them.
    assert_eq!(
        calls(
            "SELECT kv_get('a') AS a, kv_set('b', 1) AS b, kv_get('c') AS c FROM \"t/#\" \
             WHERE kv_get('w') = 1",
            "{}"
        ),
        [
            "rule:test get a",
            "rule:test set b",
            "rule:test get c",
            "rule:test get w"
        ]
    );
}

/// `AND` and `OR` stop at the operand that decides (EMQX's `andalso` and `orelse`), a
/// `CASE` evaluates only the branch it takes, and the conditions before it.
#[test]
fn a_call_behind_and_or_or_an_untaken_case_branch_does_not_run() {
    let where_ = |cond: &str| {
        calls(
            &format!("SELECT 1 AS x FROM \"t/#\" WHERE {cond}"),
            "{\"x\":1}",
        )
    };
    assert_eq!(where_("1 = 2 AND kv_set('a', 1) = 1"), Vec::<String>::new());
    assert_eq!(where_("1 = 1 OR kv_set('a', 1) = 1"), Vec::<String>::new());
    assert_eq!(where_("1 = 1 AND kv_set('a', 1) = 1"), ["rule:test set a"]);
    assert_eq!(where_("1 = 2 OR kv_set('a', 1) = 1"), ["rule:test set a"]);
    assert_eq!(
        where_("kv_set('a', 1) = 1 AND kv_set('b', 1) = 2 AND kv_set('c', 1) = 1"),
        ["rule:test set a", "rule:test set b"]
    );
    let select = |expr: &str| calls(&format!("SELECT {expr} AS c FROM \"t/#\""), "{\"x\":1}");
    assert_eq!(
        select("CASE WHEN payload.x = 1 THEN kv_set('t', 1) ELSE kv_set('e', 1) END"),
        ["rule:test set t"]
    );
    assert_eq!(
        select("CASE WHEN payload.x = 2 THEN kv_set('t', 1) ELSE kv_set('e', 1) END"),
        ["rule:test set e"]
    );
    assert_eq!(
        select("CASE payload.x WHEN 1 THEN kv_set('one', 1) WHEN 2 THEN kv_set('two', 1) END"),
        ["rule:test set one"]
    );
    // The conditions run until one holds; the ones after it do not.
    assert_eq!(
        select(
            "CASE WHEN kv_set('c1', 1) = 2 THEN 'a' WHEN kv_set('c2', 1) = 1 THEN 'b' \
             WHEN kv_set('c3', 1) = 1 THEN 'c' END"
        ),
        ["rule:test set c1", "rule:test set c2"]
    );
    // The value compared (once per `WHEN` tried), and no branch but the one taken.
    assert_eq!(
        select("CASE kv_set('on', 1) WHEN 1 THEN 'yes' ELSE kv_set('else', 1) END"),
        ["rule:test set on"]
    );
}

/// The calls of a statement on [`CONDITIONS_PAYLOAD`] as `<op> <key>`, in order,
/// without the table's name or the key's encoding.
fn ops(sql: &str) -> (Result<Vec<String>, String>, String) {
    let (out, log) = run(sql, CONDITIONS_PAYLOAD);
    let log: Vec<String> = log
        .iter()
        .map(|call| {
            let op = call.split(' ').nth(1).unwrap();
            format!("{op} {}", call.chars().last().unwrap())
        })
        .collect();
    (out, log.join(", "))
}

/// The message EMQX was probed with for the two tests below.
const CONDITIONS_PAYLOAD: &str = r#"{"flag":true,"f":false,"n":1,"s":"a","arr":[1,2,3]}"#;

/// `CASE x WHEN v THEN …` is EMQX's `eval_switch_clauses`: for each `WHEN` it comes to,
/// `v` is evaluated and then `x`. So a call in `x` runs once per `WHEN` tried, each
/// time after that `WHEN`'s own calls, and not at all when the first `v` fails. Each
/// row is the calls EMQX 6.3.1 made (`kv_store_put`, read back from the node's table
/// after `emqx_rule_sqltester:test/1`) and the field it gave.
#[test]
fn a_case_operand_is_evaluated_after_each_when_as_emqx_does() {
    for (expr, value, calls) in [
        // Two `WHEN`s tried: the operand twice, so the counter in it ends at 2.
        (
            "CASE kv_store_put('L', kv_store_get('L', 0) + 1) WHEN 5 THEN 'a' WHEN 6 THEN 'b' \
             ELSE 'c' END",
            Some("c"),
            "get L, set L, get L, set L",
        ),
        // The first `WHEN` matches (`kv_store_put` gives `true`): the operand once.
        (
            "CASE kv_store_put('L', kv_store_get('L', 0) + 1) WHEN true THEN 'a' WHEN 6 THEN 'b' \
             ELSE 'c' END",
            Some("a"),
            "get L, set L",
        ),
        // The `WHEN`'s value before the operand.
        (
            "CASE kv_store_put('L', 1) WHEN kv_store_put('R', 1) THEN 'y' ELSE 'n' END",
            Some("y"),
            "set R, set L",
        ),
        // A `WHEN` that fails leaves the operand uncalled, and its error is the rule's.
        (
            "CASE kv_store_put('L', 1) WHEN abs('x') THEN 'a' ELSE 'c' END",
            None,
            "",
        ),
        // An operand that fails does so after the `WHEN`'s call.
        (
            "CASE abs('x') WHEN kv_store_put('R', 1) THEN 'a' ELSE 'c' END",
            None,
            "set R",
        ),
        // The last two rows follow from `eval_switch_clauses` as decompiled, and were
        // not probed on their own. The second `WHEN` fails after the operand ran for
        // the first.
        (
            "CASE kv_store_put('L', 1) WHEN 5 THEN 'a' WHEN abs('x') THEN 'b' ELSE 'c' END",
            None,
            "set L",
        ),
        // A `WHEN` after the one that matched is not evaluated, nor the operand again.
        (
            "CASE kv_store_put('L', 1) WHEN true THEN 'a' WHEN kv_store_put('R', 1) THEN 'b' END",
            Some("a"),
            "set L",
        ),
    ] {
        let (out, log) = ops(&format!("SELECT {expr} AS a FROM \"t/#\""));
        let want = value.map(|v| vec![format!("{{\"a\":\"{v}\"}}")]);
        assert_eq!(out.clone().ok(), want, "{expr}: {out:?}");
        assert_eq!(log, calls, "{expr}");
    }
    // Which error surfaces where both fail: the `WHEN`'s, evaluated first.
    let e = error(
        "SELECT CASE abs('x') WHEN -payload.s THEN 1 END AS a FROM \"t/#\"",
        CONDITIONS_PAYLOAD,
    );
    assert!(e.contains("cannot negate"), "{e}");
    let e = error(
        "SELECT CASE -payload.s WHEN abs('x') THEN 1 END AS a FROM \"t/#\"",
        CONDITIONS_PAYLOAD,
    );
    assert!(e.starts_with("abs(): "), "{e}");
}

/// Conditions EMQX fails and this engine evaluates (ADR 0083, amendment of 2026-10-11;
/// `tests_conditions` has the outcomes): what runs here where EMQX has already failed
/// the rule. Each row carries the calls EMQX 6.3.1 made, probed as above. A row whose
/// calls differ is a rule that does not work in EMQX at all, so no rule that works
/// there sees the difference.
#[test]
fn a_condition_emqx_fails_goes_on_to_the_calls_behind_it() {
    // (condition, passes, calls here, EMQX's calls; EMQX fails the rule unless noted)
    for (cond, passes, calls, _emqx_calls) in [
        // A bare value on the left of an `OR` that is not `true`: the right side runs.
        ("payload.f OR kv_store_put('R', 1)", true, "set R", ""),
        // One that is `true` decides, as a comparison would.
        ("payload.flag OR kv_store_put('R', 1)", true, "", ""),
        ("payload.f AND kv_store_put('R', 1)", false, "", ""),
        // On the right, EMQX fails after the left side's call too.
        (
            "kv_store_put('L', 1) AND payload.flag",
            true,
            "set L",
            "set L",
        ),
        // `NOT IN` evaluates its list whole, as `IN` does; so does the portable
        // `NOT contains` (EMQX passes that one: the same calls).
        (
            "payload.s NOT IN ('x', kv_store_put('L', 1))",
            true,
            "set L",
            "",
        ),
        (
            "NOT (payload.s IN ('x', kv_store_put('L', 1)))",
            true,
            "set L",
            "",
        ),
        (
            "NOT contains(payload.s, ['x', kv_store_put('L', 1)])",
            true,
            "set L",
            "set L",
        ),
        (
            "NOT (kv_store_put('L', 1) AND kv_store_put('R', 1))",
            false,
            "set L, set R",
            "",
        ),
        // A function's number: "not true" here, so an `OR` goes on.
        ("abs(1) OR kv_store_put('R', 1)", true, "set R", ""),
        ("abs(1) AND kv_store_put('R', 1)", false, "", ""),
        (
            "kv_store_put('L', 1) AND abs(1) AND kv_store_put('R', 1)",
            false,
            "set L",
            "set L",
        ),
    ] {
        let (out, log) = ops(&format!("SELECT 1 AS a FROM \"t/#\" WHERE {cond}"));
        assert_eq!(out.clone().map(|o| !o.is_empty()), Ok(passes), "{cond}");
        assert_eq!(log, calls, "{cond}");
    }
    // `CASE x WHEN <NOT …>`: EMQX fails before either call; here the `WHEN` is `true`.
    let (out, log) = ops(
        "SELECT CASE kv_store_put('L', 1) WHEN NOT payload.f THEN 'y' ELSE 'n' END AS a \
         FROM \"t/#\"",
    );
    assert_eq!(out.unwrap(), [r#"{"a":"y"}"#]);
    assert_eq!(log, "set L");
    // An `INCASE` with a bare right side of `OR`: EMQX runs the first element's `DO`
    // ("set L") and fails the rule at the second; here the others are turned away.
    let (out, log) = ops(
        "FOREACH payload.arr DO item AS v, kv_store_put('L', item) AS w INCASE item < 2 OR item \
         FROM \"t/#\"",
    );
    assert_eq!(out.unwrap().len(), 1);
    assert_eq!(log, "set L");
}

/// EMQX evaluates an `IN` list whole before it looks for the value (probed on 6.3.1:
/// `1 IN (1, kv_store_put('inl', 1))` writes, and `1 IN (1, abs('x'))` fails the rule).
#[test]
fn an_in_list_is_evaluated_whole() {
    assert_eq!(
        calls(
            "SELECT 1 AS x FROM \"t/#\" WHERE 1 IN (1, kv_set('a', 1), kv_set('b', 1))",
            "{}"
        ),
        ["rule:test set a", "rule:test set b"]
    );
    let (out, log) = run(
        "SELECT 1 AS x FROM \"t/#\" WHERE 5 IN (kv_set('a', 1), 2, kv_set('b', 1))",
        "{}",
    );
    assert_eq!(out.unwrap(), Vec::<String>::new());
    assert_eq!(log.len(), 2);
    let e = error("SELECT 1 AS x FROM \"t/#\" WHERE 1 IN (1, abs('x'))", "{}");
    assert!(e.starts_with("abs(): "), "{e}");
    let e = error("SELECT 1 AS x FROM \"t/#\" WHERE 1 IN (abs('x'), 1)", "{}");
    assert!(e.starts_with("abs(): "), "{e}");
}

/// Nothing is rolled back: a write stays when a later field, or the `WHERE`, fails the
/// rule; a field that fails first stops the ones after it.
#[test]
fn a_write_stays_when_a_later_field_fails_the_rule() {
    let (out, log) = run(
        "SELECT kv_set('before', 1) AS p, abs('x') AS q, kv_set('after', 1) AS r FROM \"t/#\"",
        "{}",
    );
    assert!(out.unwrap_err().starts_with("abs(): "));
    assert_eq!(log, ["rule:test set before"]);
    let (out, log) = run(
        "SELECT kv_set('before', 1) AS p FROM \"t/#\" WHERE abs('x') = 1",
        "{}",
    );
    assert!(out.is_err());
    assert_eq!(log, ["rule:test set before"]);
    // A function's arguments run left to right, each before the call itself.
    assert_eq!(
        calls(
            "SELECT kv_set(kv_get('k1'), kv_get('v1'), kv_incr('t')) AS v FROM \"t/#\"",
            "{}"
        ),
        ["rule:test get k1", "rule:test get v1", "rule:test incr t"]
    );
}

/// In a `FOREACH`: the collection expressions once, the `WHERE`, then per element the
/// `INCASE` and, for an element that passed, the `DO`.
#[test]
fn foreach_calls_run_per_element() {
    let payload = r#"{"a":["x","y","z"]}"#;
    assert_eq!(
        calls(
            "FOREACH kv_set('coll', 1) AS c, payload.a DO kv_set('do_' + item, 1) AS p \
             INCASE kv_set('in_' + item, 1) = 1 AND item <> 'y' FROM \"t/#\" \
             WHERE kv_set('where', 1) = 1",
            payload
        ),
        [
            "rule:test set coll",
            "rule:test set where",
            "rule:test set in_x",
            "rule:test set do_x",
            "rule:test set in_y",
            "rule:test set in_z",
            "rule:test set do_z",
        ]
    );
    // A false WHERE: the collection's calls have run, no element's.
    assert_eq!(
        calls(
            "FOREACH payload.a, kv_set('coll', 1) AS c DO kv_set(item, 1) AS p FROM \"t/#\" \
             WHERE 1 = 2",
            payload
        ),
        ["rule:test set coll"]
    );
    // A DO that fails on the second element leaves the first one's write.
    let (out, log) = run(
        "FOREACH payload.a DO kv_set(item, 1) AS p, abs(item) AS q FROM \"t/#\"",
        payload,
    );
    assert!(out.is_err());
    assert_eq!(log, ["rule:test set x"]);
}

// -- what the engine bounds --------------------------------------------------------------

fn counting_rules(store: &Arc<Recorder>) -> RuleSet {
    // Each rule makes one call per element of the message's array.
    let text = r#"
[rules.a]
sql = '''FOREACH payload.a INCASE kv_incr('n') < 0 FROM "t/#"'''
[rules.b]
sql = '''FOREACH payload.a INCASE kv_incr('n') < 0 FROM "t/#"'''
"#;
    load(text, store).unwrap().rules
}

fn array(n: usize) -> String {
    format!(r#"{{"a":[{}]}}"#, vec!["1"; n].join(","))
}

/// D9: at most 1,024 state calls for one message, read or write, across every rule it
/// matches; the 1,025th fails its rule, and the calls before it stay.
#[test]
fn the_1025th_state_call_of_a_message_fails_its_rule() {
    let store = Arc::new(Recorder::default());
    let set = counting_rules(&store);
    // 512 + 512: both rules run to the end.
    let results = publish(&set, "t/1", &array(512));
    assert_eq!(results.len(), 2, "{results:?}");
    assert!(results.iter().all(|r| !r.contains("failed")), "{results:?}");
    assert_eq!(log_of(&store).len(), 1024);
    // 600 + 600: the second rule's 425th call is the message's 1,025th.
    let results = publish(&set, "t/1", &array(600));
    assert!(!results[0].contains("failed"), "{results:?}");
    assert_eq!(
        results[1],
        "b failed: kv_incr(): this message already made 1024 state calls (across every rule \
         it matched and every message they republished); the rest fail"
    );
    assert_eq!(log_of(&store).len(), 1024);
    // Each message has a budget of its own.
    assert!(publish(&set, "t/1", &array(10))
        .iter()
        .all(|r| !r.contains("failed")));
}

/// D8: a `kv_incr` with an idempotency key writes two entries, the count and the
/// remembered key, and is two of the message's 1,024 calls.
#[test]
fn a_kv_incr_with_an_idempotency_key_is_two_of_a_messages_calls() {
    let store = Arc::new(Recorder::default());
    let text = "[rules.a]\nsql = '''FOREACH payload.a INCASE kv_incr('n', 1, 60, 'k') < 0 \
                FROM \"t/#\"'''\n";
    let set = load(text, &store).unwrap().rules;
    let results = publish(&set, "t/1", &array(512));
    assert_eq!(results, ["a no_result"]);
    assert_eq!(log_of(&store).len(), 512);
    let results = publish(&set, "t/1", &array(513));
    assert_eq!(
        results,
        [
            "a failed: kv_incr(): this message already made 1024 state calls (across every rule \
          it matched and every message they republished); the rest fail"
        ]
    );
    assert_eq!(log_of(&store).len(), 512);
}

/// The budget is the original message's: a message its rules republished is evaluated
/// within it, so a chain of rules makes no more calls than one rule may.
#[test]
fn a_republished_message_counts_its_state_calls_in_the_originals_budget() {
    let store = Arc::new(Recorder::default());
    let set = counting_rules(&store);
    let payload = Bytes::from(array(400));
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
    let budget = Budget::new(payload.len());
    let mut failed = Vec::new();
    let mut report = |rule: &Rule, outcome: Outcome<'_>| {
        if let Outcome::Failed(e) = outcome {
            failed.push(format!("{}: {e}", rule.id()));
        }
    };
    let mut out = Vec::new();
    set.on_publish_within(&input, &budget, &mut report, &mut out);
    assert_eq!(budget.state_calls(), 800);
    // The re-entered message: 224 calls are left of the original's 1,024.
    set.on_publish_within(&input, &budget, &mut report, &mut out);
    assert_eq!(budget.state_calls(), 1024);
    assert_eq!(failed.len(), 2, "{failed:?}");
    assert!(failed[0].starts_with("a: kv_incr(): this message already made 1024 state calls"));
    assert_eq!(log_of(&store).len(), 1024);
}

/// D9: what a read returns is charged whole to the message's 1 MiB build budget, as a
/// `jq` call's output is, so a `FOREACH` cannot multiply stored values into a large
/// message.
#[test]
fn a_read_is_charged_to_the_build_budget() {
    let store = Arc::new(Recorder {
        read_bytes: 4096,
        ..Recorder::default()
    });
    let functions = with_store(&store);
    let payload = Bytes::from(array(300));
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
    // 256 reads of 4 KiB are the whole megabyte; the 257th is past it.
    let sql = "FOREACH payload.a INCASE kv_get('big') = 1 FROM \"t/#\"";
    let e = test_sql_with(sql, &input, &functions).unwrap_err();
    assert_eq!(
        e,
        "kv_get(): the values this message's rules read from state would pass 1048576 bytes \
         (the budget is per message, shared with every function call)"
    );
    assert_eq!(log_of(&store).len(), 257);
    // The budget is the one every function shares: a read leaves less for `pad`.
    let sql = "SELECT kv_store_get('big') AS v, pad('x', 1048000, 'trailing') AS p FROM \"t/#\"";
    let e = test_sql_with(sql, &input, &functions).unwrap_err();
    assert!(e.contains("would build more than 1048576 bytes"), "{e}");
    // A write reads nothing and is charged nothing.
    let sql = "FOREACH payload.a INCASE kv_set('k', 1) = 2 FROM \"t/#\"";
    assert!(test_sql_with(sql, &input, &functions).is_ok());
}

// -- dry runs --------------------------------------------------------------------------

/// D11: a dry run's state calls go to the scratch store it was given, under each
/// namespace's own name and settings, and never to the store the rules were loaded
/// with; rules of one dry run see each other's writes.
#[test]
fn a_dry_run_never_touches_the_store_the_rules_were_loaded_with() {
    let running = Arc::new(Recorder::default());
    let text = r#"
[state.shared]
max_keys = 3
[rules.w]
state = "shared"
sql = '''SELECT kv_set('k', 5) AS v, kv_store_put('p', 1) AS p FROM "t/#"'''
[rules.r]
state = "shared"
sql = '''SELECT kv_get('k') AS v FROM "t/#"'''
"#;
    let set = load(text, &running).unwrap().rules;
    log_of(&running);
    let payload = Bytes::from_static(b"{}");
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
    let scratch = Arc::new(Recorder::default());
    let mut console = Vec::new();
    for id in ["w", "r"] {
        let mut out = Vec::new();
        assert!(set.evaluate_one(
            id,
            &input,
            Some(&*scratch),
            &mut |_, outcome| {
                if let Outcome::Failed(e) = outcome {
                    panic!("{id}: {e}");
                }
            },
            &mut out,
        ));
        console.push(out.len());
    }
    assert_eq!(log_of(&running), Vec::<String>::new());
    assert_eq!(log_of(&scratch).len(), 3);
    // The second rule read what the first wrote, in the scratch store.
    let written = scratch
        .data
        .lock()
        .unwrap()
        .get(&("shared".to_string(), b"k".to_vec()))
        .map(|v| v.to_json().unwrap());
    assert_eq!(written.as_deref(), Some("5"));
    let opened = scratch.opened.lock().unwrap().clone();
    // Each rule asks the scratch store for its namespace by name: the same one.
    assert_eq!(
        opened.iter().map(|(n, _)| &**n).collect::<Vec<_>>(),
        ["shared", "kv_store", "shared"]
    );
    assert_eq!(opened[0].1.max_keys, 3);
    // Without a scratch store, a state call fails the rule, and the store the rules
    // were loaded with is still not touched.
    let mut failure = None;
    set.evaluate_one(
        "w",
        &input,
        None,
        &mut |_, outcome| {
            if let Outcome::Failed(e) = outcome {
                failure = Some(e.to_string());
            }
        },
        &mut Vec::new(),
    );
    assert_eq!(
        failure.as_deref(),
        Some("kv_set(): a dry run was given no scratch store, so its rules cannot call state")
    );
    assert_eq!(log_of(&running), Vec::<String>::new());
}

/// A statement on its own (`mqttd --rule-test`) is under the `[state.default]` and
/// `[state.kv_store]` settings of the rules file it is tried beside.
#[test]
fn a_statement_alone_takes_the_files_default_settings() {
    let store = Arc::new(Recorder::default());
    let functions = with_store(&store)
        .declaring("[state.default]\nmax_keys = 9\n[state.kv_store]\nmax_keys = 4\n")
        .unwrap();
    let payload = Bytes::from_static(b"{}");
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
    let sql = "SELECT kv_get('k') AS a, kv_store_get('k') AS b FROM \"t/#\"";
    test_sql_with(sql, &input, &functions).unwrap();
    let opened = store.opened.lock().unwrap().clone();
    assert_eq!(opened[0].0, "rule:test");
    assert_eq!(opened[0].1.max_keys, 9);
    assert_eq!(opened[1].0, "kv_store");
    assert_eq!(opened[1].1.max_keys, 4);
}

/// ADR 0084's per-rule edit leaves a rule's `state` key, and the `[state]` tables, as
/// they are.
#[test]
fn a_per_rule_edit_keeps_the_rules_state_key() {
    let text = "[state.alerts]\nmax_keys = 5\n\n[rules.r]\nstate = \"alerts\"\n\
                sql = '''SELECT kv_incr('n') AS n FROM \"t/#\"'''\n";
    let edited = edit::put_rule(
        text,
        "r",
        &edit::RuleEdit {
            sql: "SELECT kv_incr('m') AS n FROM \"t/#\"".into(),
            actions: Vec::new(),
            description: String::new(),
            enable: true,
        },
    )
    .unwrap();
    assert!(edited.contains("state = \"alerts\""), "{edited}");
    assert!(edited.contains("kv_incr('m')"), "{edited}");
    assert!(
        edited.starts_with("[state.alerts]\nmax_keys = 5\n"),
        "{edited}"
    );
}

/// What shows a store's keys reads a `kv_store_*` key back as the value it was made
/// from (`key_term`), and refuses bytes `term_key` never wrote.
#[test]
fn a_term_key_reads_back_as_the_value_it_was_made_from() {
    use crate::state::key_term;
    for text in [
        "null",
        "true",
        "false",
        "0",
        "-7",
        "1.0",
        "-0.0",
        "123456789012345678901234567890",
        "-123456789012345678901234567890",
        "\"\"",
        "\"text\"",
        "[]",
        "[1,[2.5,\"x\"],{\"a\":null}]",
        "{}",
        "{\"a\":1,\"b\":{\"c\":[true]}}",
    ] {
        let value = json_decode(text.as_bytes()).unwrap();
        let key = term_key(&value);
        let back = key_term(&key).unwrap_or_else(|| panic!("{text} reads back"));
        assert!(back.exact_eq(&value), "{text} came back as {back:?}");
        assert_eq!(term_key(&back), key, "{text}");
    }
    assert!(matches!(
        key_term(&term_key(&Value::Undefined)),
        Some(Value::Undefined)
    ));
    // A binary that is not text stays a binary.
    let binary = Value::Bin(Bytes::from_static(b"\xff\x00"));
    assert!(key_term(&term_key(&binary)).unwrap().exact_eq(&binary));
    // A map reads back in the order of its keys, whatever order it was written in.
    let map = json_decode(br#"{"b":1,"a":2}"#).unwrap();
    assert_eq!(
        key_term(&term_key(&map)).unwrap().to_json().unwrap(),
        r#"{"a":2,"b":1}"#
    );
    // Not a key: nothing, a tag no value has, a value cut short, bytes left over, a
    // length past what is there.
    let text = term_key(&Value::from("text"));
    for bad in [
        &[][..],
        &[42],
        &text[..text.len() - 1],
        &[&text[..], &[0]].concat(),
        &[7, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        &[8, 0, 0, 0, 0, 0, 0, 0, 2, 1],
    ] {
        assert!(key_term(bad).is_none(), "{bad:?}");
    }
}

/// A replayed sequence's clock (D11): an input that carries a time is evaluated at
/// that time, by every function that reads the clock and in `timestamp`; one that
/// carries none reads the wall clock, as every trigger of a running broker does.
#[test]
fn an_input_replayed_at_a_time_is_evaluated_at_that_time() {
    let sql = "SELECT now_timestamp() AS s, now_timestamp('millisecond') AS ms, \
               now_timestamp('microsecond') AS us, now_timestamp('nanosecond') AS ns, \
               rfc3339_to_unix_ts(now_rfc3339('millisecond'), 'millisecond') AS text, \
               format_date('second', '+00:00', '%Y-%m-%d %H:%M:%S') AS date, \
               timestamp, publish_received_at FROM \"t/#\"";
    let payload = Bytes::from_static(b"{}");
    let props = mqtt_core::AppProperties::default();
    let mut input = PublishInput::new("c", "t/1", &payload, 0, &props);
    input.clock_ms = Some(1_735_689_601_250);
    input.received_at_ms = Some(1_735_689_601_250);
    assert_eq!(Input::clock_ms(&input), Some(1_735_689_601_250));
    assert_eq!(
        test_sql(sql, &input).unwrap(),
        [
            "{\"s\":1735689601,\"ms\":1735689601250,\"us\":1735689601250000,\
             \"ns\":1735689601250000000,\"text\":1735689601250,\
             \"date\":\"2025-01-01 00:00:01\",\"timestamp\":1735689601250,\
             \"publish_received_at\":1735689601250}"
        ]
    );
    // Without one: the wall clock.
    let live = PublishInput::new("c", "t/1", &payload, 0, &props);
    assert_eq!(Input::clock_ms(&live), None);
    let before = now_ms();
    let out = test_sql(
        "SELECT now_timestamp('millisecond') AS ms, timestamp FROM \"t/#\"",
        &live,
    )
    .unwrap();
    let Value::Map(out) = json_decode(out[0].as_bytes()).unwrap() else {
        panic!("an object: {out:?}")
    };
    for field in ["ms", "timestamp"] {
        let Some(Value::Int(at)) = out.get(field) else {
            panic!("{field} is a time: {out:?}")
        };
        assert!((before..=now_ms()).contains(at), "{field} = {at}");
    }
}

/// A sample event raised at a time of a sequence's carries that time in every field
/// that is one, and is the same event however often it is built.
#[test]
fn a_sample_event_replayed_at_a_time_carries_that_time() {
    let client = ClientInfo {
        clientid: "c",
        username: None,
        peer: None,
        sockname: None,
        node: "n",
    };
    let payload = Bytes::from_static(b"{}");
    let at = 1_735_689_600_000;
    let fields = |kind| {
        let event = EventInput::sample_message_at(kind, &client, "t/1", 1, &payload, at, 7);
        assert_eq!(event.clock_ms(), Some(at));
        Value::from(event.all_fields()).to_json().unwrap()
    };
    for kind in EventKind::ALL {
        let text = fields(kind);
        assert_eq!(text, fields(kind), "{kind:?} is the same twice");
        let Value::Map(map) = json_decode(text.as_bytes()).unwrap() else {
            panic!("an object: {text}")
        };
        assert!(
            matches!(map.get("timestamp"), Some(Value::Int(t)) if *t == at),
            "{text}"
        );
        for (field, value) in map.iter() {
            let is_time = field.ends_with("_at") || &**field == "timestamp";
            if let (true, Value::Int(t)) = (is_time, value) {
                // A sample disconnect is of a connection made a second before.
                assert!(*t == at || *t == at - 1000, "{kind:?} {field} = {t}");
            }
        }
        if EventKind::MESSAGE.contains(&kind) {
            assert!(
                text.contains("\"id\":\"00000000000000000000000000000007\""),
                "{text}"
            );
        }
    }
    // The wall clock's sample reads no sequence's time.
    assert_eq!(
        EventInput::sample(EventKind::ClientConnected, &client, "t/1", 0).clock_ms(),
        None
    );
}

/// One statement compiled once runs against many inputs on the store it was compiled
/// with, so what one input wrote the next reads (`SqlTest`); an input its `FROM` does
/// not select is told apart from a failure.
#[test]
fn a_statement_compiled_once_keeps_its_state_from_one_input_to_the_next() {
    let store = Arc::new(Recorder::default());
    let test = SqlTest::compile(
        "SELECT kv_incr(clientid) AS n, proc_dict_put('seen', topic) AS before FROM \"t/#\"",
        &with_store(&store),
    )
    .unwrap();
    assert!(test.selects_topics() && test.events().is_empty());
    let payload = Bytes::from_static(b"{}");
    let props = mqtt_core::AppProperties::default();
    let dictionary = crate::state::Dictionary::new();
    let run = |topic: &str, dictionary: &crate::state::Dictionary| {
        test.run(
            &PublishInput::new("c", topic, &payload, 0, &props),
            dictionary,
        )
    };
    assert_eq!(
        run("t/1", &dictionary).unwrap(),
        [r#"{"n":1,"before":"undefined"}"#]
    );
    assert_eq!(
        run("t/2", &dictionary).unwrap(),
        [r#"{"n":2,"before":"t/1"}"#]
    );
    // Another dictionary, the same store.
    assert_eq!(
        run("t/3", &crate::state::Dictionary::new()).unwrap(),
        [r#"{"n":3,"before":"undefined"}"#]
    );
    let other = PublishInput::new("c", "other", &payload, 0, &props);
    let why = test.from_mismatch(&other).expect("not selected");
    assert_eq!(run("other", &dictionary).unwrap_err(), why);
    assert_eq!(
        log_of(&store).len(),
        3,
        "an input not selected calls nothing"
    );

    let events = SqlTest::compile(
        "SELECT clientid FROM \"$events/client/connected\", \"$events/client/disconnected\"",
        &Functions::default(),
    )
    .unwrap();
    assert!(!events.selects_topics());
    assert_eq!(
        events.events(),
        [EventKind::ClientConnected, EventKind::ClientDisconnected]
    );
    assert!(SqlTest::compile("SELECT", &Functions::default()).is_err());
}

/// One rule selecting `from`, loaded; or why it does not load.
fn selecting(from: &str) -> Result<Loaded, String> {
    RuleSet::parse(&format!(
        "[rules.r]\nsql = 'SELECT * FROM \"{from}\"'\nactions = [{{ function = \"console\" }}]\n"
    ))
    .map_err(|e| e.to_string())
}

/// ADR 0085 D7: `$events/state/expired` is mqttd's own event, selected only by its
/// exact name. No wildcard selects it, `"$events/#"` included, so a filter keeps
/// selecting exactly what it selects in EMQX; it has no second spelling; and a
/// wildcard that could only mean it is refused, saying how to name it.
#[test]
fn the_expiry_event_is_selected_by_its_exact_name_and_by_no_wildcard() {
    let exact = selecting("$events/state/expired").unwrap();
    assert!(exact.warnings.is_empty(), "{:?}", exact.warnings);
    for kind in EventKind::ALL {
        assert_eq!(
            exact.rules.wants_event(kind),
            kind == EventKind::StateExpired,
            "{kind:?}"
        );
    }
    assert!(!exact.rules.wants_message_events());
    assert_eq!(EventKind::StateExpired.topic(), STATE_EXPIRED_TOPIC);
    assert_eq!(
        EventKind::parse("state.expired"),
        Some(EventKind::StateExpired)
    );
    assert_eq!(
        EventKind::parse("state/expired"),
        Some(EventKind::StateExpired)
    );
    assert!(!EventKind::EMQX.contains(&EventKind::StateExpired));
    assert!(!EMQX_EVENT_TOPICS
        .iter()
        .any(|(topic, _)| *topic == STATE_EXPIRED_TOPIC));

    // Wildcards that match other events load, and do not select it.
    for filter in ["$events/#", "$events/+/+", "$events/+/#", "#"] {
        let Ok(loaded) = selecting(filter) else {
            // `#` alone is a topic filter, not an event filter.
            assert_eq!(filter, "#");
            continue;
        };
        assert!(
            !loaded.rules.wants_event(EventKind::StateExpired),
            "{filter} must not select mqttd's own event"
        );
        assert!(!EventKind::matching(filter)
            .kinds
            .contains(&EventKind::StateExpired));
    }
    let all = selecting("$events/#").unwrap();
    assert!(EventKind::EMQX.iter().all(|k| all.rules.wants_event(*k)));
    assert!(
        !all.warnings.iter().any(|w| w.contains("state")),
        "a rule ported from EMQX is told nothing new: {:?}",
        all.warnings
    );

    // Wildcards that could only mean it are refused, naming the exact topic.
    for filter in ["$events/state/#", "$events/state/+", "$events/+/expired"] {
        let refused = selecting(filter).unwrap_err();
        assert!(
            refused.contains("matches no event mqttd raises")
                && refused.contains(
                    "\"$events/state/expired\" is mqttd's own and is selected only by its exact \
                     name"
                ),
            "{filter}: {refused}"
        );
    }
    // No underscore spelling: EMQX has none to be compatible with.
    let refused = selecting("$events/state_expired").unwrap_err();
    assert!(refused.contains("is not a supported event"), "{refused}");

    // Listed together with a wildcard, each selects its own.
    let both = RuleSet::parse(
        "[rules.r]\nsql = 'SELECT * FROM \"$events/client/+\", \"$events/state/expired\"'\n\
         actions = [{ function = \"console\" }]\n",
    )
    .unwrap();
    assert!(both.rules.wants_event(EventKind::StateExpired));
    assert!(both.rules.wants_event(EventKind::ClientConnected));
    assert!(!both.rules.wants_event(EventKind::MessageDropped));
}

/// ADR 0085 D7: the event's fields. `event`, `namespace`, `key`, `value` (what
/// `kv_get` read last), `type`, `expired_at`, `timestamp`, `node`, and the rule's own
/// `metadata.rule_id`; it is about no client. A replayed one carries the sequence's
/// time.
#[test]
fn the_expiry_event_carries_the_key_its_value_and_when_it_expired() {
    let expiry = StateExpiry {
        namespace: "seen",
        key: b"seen:d1",
        value: Value::Int(41),
        kind: "sum",
        expired_at_ms: 1_735_689_600_000,
    };
    let before = now_ms();
    let event = EventInput::state_expired("n1", &expiry);
    assert_eq!(event.kind(), EventKind::StateExpired);
    assert_eq!(event.clock_ms(), None);
    let mut names: Vec<String> = event
        .all_fields()
        .iter()
        .map(|(k, _)| k.to_string())
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "event",
            "expired_at",
            "key",
            "namespace",
            "node",
            "timestamp",
            "type",
            "value"
        ]
    );
    let Value::Int(raised) = event.field("timestamp") else {
        panic!("timestamp is a time")
    };
    assert!((before..=now_ms()).contains(&raised));

    let at = 1_735_689_601_000;
    let event = EventInput::state_expired_at("n1", &expiry, at);
    assert_eq!(event.clock_ms(), Some(at));
    let out = test_sql(
        "SELECT event, namespace, key, value, type, expired_at, timestamp, node, \
         metadata.rule_id AS rule, clientid, timestamp - expired_at AS late \
         FROM \"$events/state/expired\" WHERE value > 40",
        &event,
    )
    .unwrap();
    assert_eq!(
        out,
        [
            r#"{"event":"state.expired","namespace":"seen","key":"seen:d1","value":41,"type":"sum","expired_at":1735689600000,"timestamp":1735689601000,"node":"n1","rule":"test","clientid":"undefined","late":1000}"#
        ]
    );
    // A statement that selects other events does not run on it.
    let other = SqlTest::compile("SELECT * FROM \"$events/#\"", &Functions::default()).unwrap();
    assert_eq!(
        other.from_mismatch(&event).as_deref(),
        Some("the statement's FROM does not select the state.expired event")
    );
    // A key that is not UTF-8 is its bytes; a set's value is its elements.
    let binary = EventInput::state_expired(
        "n1",
        &StateExpiry {
            key: &[0xff, 0x00],
            value: Value::from(vec![Value::Int(1), Value::from("a")]),
            kind: "union",
            ..expiry
        },
    );
    assert!(matches!(binary.field("key"), Value::Bin(_)));
    assert_eq!(binary.field("value").to_json().unwrap(), r#"[1,"a"]"#);
}

/// `mqttd --rule-test --event state.expired` and a dry run of a rule on the event:
/// the sample is a key named by the topic, in the namespace `sample`, that held the
/// payload and expired just now.
#[test]
fn a_sample_of_the_expiry_event_is_a_key_named_by_the_topic_holding_the_payload() {
    let client = ClientInfo {
        clientid: "c",
        username: None,
        peer: None,
        sockname: None,
        node: "n",
    };
    let sample = EventInput::sample_message(
        EventKind::StateExpired,
        &client,
        "seen:d1",
        0,
        &Bytes::from_static(b"42"),
    );
    let out = test_sql(
        "SELECT namespace, key, value, type, expired_at = timestamp AS now, node \
         FROM \"$events/state/expired\"",
        &sample,
    )
    .unwrap();
    assert_eq!(
        out,
        [
            r#"{"namespace":"sample","key":"seen:d1","value":"42","type":"lww","now":true,"node":"n"}"#
        ]
    );
}
