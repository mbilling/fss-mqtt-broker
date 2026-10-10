//! A republished message re-entering the rule engine, as EMQX's does
//! (`emqx_rule_actions:republish/3`, `safe_publish/7`). The broker drives the re-entry
//! (`mqttd::rules`); this is what the engine contributes: the message as the rules see
//! it, `direct_dispatch`, the same-rule guard, the depth guard and the shared budget.
//! Expected values are what EMQX 6.3.1 showed for the same rules.

use super::*;

fn set(text: &str) -> RuleSet {
    RuleSet::parse(text).unwrap().rules
}

/// Evaluate `input` within `budget`: the effects, and every outcome but timing.
fn run(
    set: &RuleSet,
    input: &PublishInput<'_>,
    budget: &Budget,
) -> (Vec<(Arc<str>, Effect)>, Vec<String>) {
    let mut out = Vec::new();
    let mut log = Vec::new();
    set.on_publish_within(
        input,
        budget,
        &mut |r, o| {
            let what = match o {
                Outcome::Elapsed(_) => return,
                Outcome::Passed => "passed".to_string(),
                Outcome::NoResult => "no_result".to_string(),
                Outcome::Failed(e) => format!("failed({e})"),
                Outcome::ActionOk => "action_ok".to_string(),
                Outcome::ActionFailed(e) => format!("action_failed({e})"),
                Outcome::Recursive(g) => format!("recursive({})", g.as_str()),
            };
            log.push(format!("{}:{what}", r.id()));
        },
        &mut out,
    );
    (out, log)
}

fn republish(e: &(Arc<str>, Effect)) -> &Republish {
    match &e.1 {
        Effect::Republish(r) => r,
        Effect::Console(c) => panic!("expected a republish, got console {c}"),
    }
}

/// A client's publish on `topic`.
fn publish<'a>(topic: &'a str, payload: &'a Bytes, props: &'a AppProperties) -> PublishInput<'a> {
    let mut m = PublishInput::new("c1", topic, payload, 1, props);
    m.username = Some("alice");
    m.peer = Some("10.0.0.1:5000".parse().unwrap());
    m.node = "n0";
    m
}

use mqtt_core::AppProperties;

/// The republished message as the next rule sees it: EMQX's `republish_clientinfo/1`
/// makes the rule id its `clientid` and leaves `username`, `peerhost` and `peername`
/// `undefined` (shown as such by `SELECT *`); `pub_props` are what the action set;
/// `flags` are the trigger's with the action's `retain`. EMQX 6.3.1, for `rA`
/// republishing `t/a` to `t/b` with `Content-Type` and the publisher's user properties:
/// `"clientid":"rA"`, `"username":"undefined"`, `"peerhost":"undefined"`,
/// `"peername":"undefined"`, `"flags":{"retain":false,"dup":false}`, `"qos":1`, and
/// `pub_props` with `User-Property`, `User-Property-Pairs` and `Content-Type`.
#[test]
fn a_republished_message_is_seen_as_emqx_shows_it() {
    let rules = set(r#"
[rules.rA]
sql = 'SELECT * FROM "t/a"'
actions = [{ function = "republish", args = { topic = "t/b", qos = 1, retain = false, payload = "${payload}-A", mqtt_properties = { "Content-Type" = "text/x" }, user_properties = "${pub_props.'User-Property'}" } }]

[rules.rB]
sql = 'SELECT * FROM "t/b"'
actions = [{ function = "republish", args = { topic = "out/b", payload = "" } }]
"#);
    let payload = Bytes::from_static(b"hello");
    let props = AppProperties {
        user_properties: vec![("k1".into(), "v1".into()), ("k2".into(), "v2".into())],
        ..AppProperties::default()
    };
    let budget = Budget::new(payload.len());
    let (out, _) = run(&rules, &publish("t/a", &payload, &props), &budget);
    assert_eq!(out.len(), 1);
    let r = republish(&out[0]);
    assert!(!r.direct_dispatch, "the default");
    assert!(r.dup_flag, "a message's republish has the message's flags");

    let input = PublishInput::republished(&out[0].0, r, 1);
    assert_eq!(input.republished_by, Some("rA"));
    let (out, _) = run(&rules, &input, &budget);
    assert_eq!(out.len(), 1, "rB runs on rA's message");
    let shown: serde_json::Value = serde_json::from_slice(&republish(&out[0]).payload).unwrap();
    for (field, want) in [
        ("clientid", serde_json::json!("rA")),
        ("username", serde_json::json!("undefined")),
        ("peerhost", serde_json::json!("undefined")),
        ("peername", serde_json::json!("undefined")),
        ("topic", serde_json::json!("t/b")),
        ("qos", serde_json::json!(1)),
        ("payload", serde_json::json!("hello-A")),
        ("flags", serde_json::json!({"retain": false, "dup": false})),
        ("client_attrs", serde_json::json!({})),
        ("event", serde_json::json!("message.publish")),
        (
            "pub_props",
            serde_json::json!({
                "User-Property": {"k1": "v1", "k2": "v2"},
                "User-Property-Pairs": [{"key": "k1", "value": "v1"}, {"key": "k2", "value": "v2"}],
                "Content-Type": "text/x",
            }),
        ),
    ] {
        assert_eq!(shown[field], want, "{field} in {shown}");
    }
    assert!(
        shown["publish_received_at"].as_i64().unwrap() > 0,
        "{shown}"
    );
}

/// A message republished from an event has `flags` of `{"retain": …}` alone: EMQX copies
/// the trigger's `flags`, and an event has none (EMQX 6.3.1: `"flags":{"retain":false}`,
/// `"pub_props":{"User-Property":{}}`). The rest of a chain started by an event keeps it.
#[test]
fn a_message_republished_from_an_event_has_no_dup_flag() {
    let rules = set(r#"
[rules.rE]
sql = 'SELECT * FROM "$events/client_connected"'
actions = [{ function = "republish", args = { topic = "e/b", payload = "ev" } }]

[rules.rEB]
sql = 'SELECT * FROM "e/#"'
actions = [{ function = "republish", args = { topic = "e/c", payload = "" } }]
"#);
    let info = ClientInfo {
        clientid: "evc",
        username: None,
        peer: None,
        sockname: None,
        node: "n0",
    };
    let ev = EventInput::client_connected(&info, &ConnInfo::sample(), 1);
    let budget = Budget::new(0);
    let mut out = Vec::new();
    rules.on_event_within(&ev, &budget, &mut |_, _| {}, &mut out);
    let r = republish(&out[0]);
    assert!(!r.dup_flag);
    let input = PublishInput::republished(&out[0].0, r, 1);
    let (out, _) = run(&rules, &input, &budget);
    let r = republish(&out[0]);
    let shown: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert_eq!(
        shown["flags"],
        serde_json::json!({"retain": false}),
        "{shown}"
    );
    assert_eq!(shown["clientid"], serde_json::json!("rE"), "{shown}");
    assert_eq!(
        shown["pub_props"],
        serde_json::json!({"User-Property": {}}),
        "{shown}"
    );
    assert!(!r.dup_flag, "carried down the chain");
}

/// A client that sent no username shows `"username":"undefined"` in `SELECT *`, as
/// EMQX's `eventmsg_publish/1` always sets the key (EMQX 6.3.1).
#[test]
fn select_star_shows_an_undefined_username() {
    let rules = set(r#"
[rules.r]
sql = 'SELECT * FROM "t/#"'
actions = [{ function = "republish", args = { topic = "o", payload = "" } }]
"#);
    let payload = Bytes::from_static(b"x");
    let props = AppProperties::default();
    let mut input = publish("t/1", &payload, &props);
    input.username = None;
    let (out, _) = run(&rules, &input, &Budget::new(1));
    let shown: serde_json::Value = serde_json::from_slice(&republish(&out[0]).payload).unwrap();
    assert_eq!(shown["username"], serde_json::json!("undefined"), "{shown}");
    assert_eq!(shown["peerhost"], serde_json::json!("10.0.0.1"), "{shown}");
}

/// EMQX's guard: a rule does not republish a message it republished itself — it logs
/// `recursive_republish_detected` and counts the action a success — but it still runs
/// on it (EMQX 6.3.1 counted `rS` matched 3 times for one publish), its other actions
/// still run, and another rule still republishes it.
#[test]
fn a_rule_does_not_republish_its_own_republished_message() {
    let rules = set(r#"
[rules.rO]
sql = 'SELECT topic, clientid FROM "s/#"'
actions = [{ function = "republish", args = { topic = "obs/s", payload = "" } }]

[rules.rS]
sql = 'SELECT * FROM "s/#"'
actions = [
  { function = "republish", args = { topic = "s/x", payload = "${payload}" } },
  { function = "console" },
  { function = "republish", args = { topic = "s/y", payload = "${payload}" } },
]
"#);
    let payload = Bytes::from_static(b"self");
    let props = AppProperties::default();
    let budget = Budget::new(payload.len());
    let (out, log) = run(&rules, &publish("s/1", &payload, &props), &budget);
    assert_eq!(out.len(), 4, "{log:?}");
    let sx = out
        .iter()
        .find(|e| matches!(&e.1, Effect::Republish(r) if r.topic == "s/x"))
        .unwrap();
    let input = PublishInput::republished(&sx.0, republish(sx), 1);
    let (out, log) = run(&rules, &input, &budget);
    assert_eq!(
        log,
        [
            "rO:passed",
            "rO:action_ok",
            "rS:passed",
            "rS:recursive(same_rule)",
            "rS:action_ok",
            "rS:recursive(same_rule)",
        ]
    );
    assert_eq!(out.len(), 2, "rO's republish and rS's console line");
    assert_eq!(republish(&out[0]).topic, "obs/s");
    assert!(matches!(out[1].1, Effect::Console(_)));
    assert_eq!(budget.effects(), 6, "the guarded actions charge nothing");

    // The guard is the rule's own message only: rO's message goes through rS.
    let input = PublishInput::republished("rO", republish(&out[0]), 2);
    let mut obs = Vec::new();
    rules.on_publish_within(&input, &budget, &mut |_, _| {}, &mut obs);
    assert!(obs.is_empty(), "obs/s matches neither FROM");
    let r = republish(&out[0]).clone();
    let s_topic = Republish {
        topic: "s/z".into(),
        ..r
    };
    let input = PublishInput::republished("rO", &s_topic, 2);
    let (out, log) = run(&rules, &input, &budget);
    assert_eq!(
        log,
        [
            "rO:passed",
            "rO:recursive(same_rule)",
            "rS:passed",
            "rS:action_ok",
            "rS:action_ok",
            "rS:action_ok",
        ]
    );
    assert_eq!(out.len(), 3);
}

/// Past [`MAX_REPUBLISH_DEPTH`] a republish is not run: EMQX has no such bound, and two
/// rules republishing into each other's `FROM` recurse there until the publisher's
/// process is killed. Below it, it runs.
#[test]
fn republishing_stops_at_the_depth_cap() {
    let rules = set(r#"
[rules.rP]
sql = 'SELECT * FROM "p/a"'
actions = [{ function = "republish", args = { topic = "p/b" } }, { function = "console" }]
"#);
    let payload = Bytes::from_static(b"loop");
    let props = AppProperties::default();
    let mut input = PublishInput::new("rQ", "p/a", &payload, 0, &props);
    input.republished_by = Some("rQ");
    input.republish_depth = MAX_REPUBLISH_DEPTH - 1;
    let budget = Budget::new(payload.len());
    let (out, log) = run(&rules, &input, &budget);
    assert_eq!(out.len(), 2, "{log:?}");
    input.republish_depth = MAX_REPUBLISH_DEPTH;
    let (out, log) = run(&rules, &input, &budget);
    assert_eq!(log, ["rP:passed", "rP:recursive(depth)", "rP:action_ok"]);
    assert_eq!(out.len(), 1, "the console line still runs");
    assert!(matches!(out[0].1, Effect::Console(_)));
}

/// One [`Budget`] covers the original and every message it leads to: the effect count
/// and the bytes carried accumulate across evaluations.
#[test]
fn the_budget_spans_every_reentry() {
    let rules = set(r#"
[rules.fan]
sql = 'FOREACH payload.xs AS x DO x AS v FROM "f/#"'
actions = [{ function = "republish", args = { topic = "f/x", payload = "${v}" } }]
"#);
    let payload = Bytes::from(format!("{{\"xs\":[{}]}}", vec!["1"; 200].join(",")));
    let props = AppProperties::default();
    let budget = Budget::new(payload.len());
    let mut log = Vec::new();
    for _ in 0..6 {
        let (_, l) = run(&rules, &publish("f/a", &payload, &props), &budget);
        log.extend(l);
    }
    assert_eq!(
        budget.effects(),
        MAX_EFFECTS_PER_TRIGGER,
        "{} effects",
        budget.effects()
    );
    assert_eq!(
        log.iter().filter(|l| l.contains("action_ok")).count(),
        MAX_EFFECTS_PER_TRIGGER
    );
    assert!(
        log.iter()
            .any(|l| l.contains("already produced 1024 effects")),
        "past the cap, actions fail"
    );
    // A fresh original has a fresh budget.
    let (out, _) = run(
        &rules,
        &publish("f/a", &payload, &props),
        &Budget::new(payload.len()),
    );
    assert_eq!(out.len(), 200);

    // Bytes too: a payload repeated past the limit stops at it, across evaluations.
    let rules = set(r#"
[rules.big]
sql = 'SELECT * FROM "b/#"'
actions = [{ function = "republish", args = { topic = "b/x", payload = "${payload}" } }]
"#);
    let big = Bytes::from(vec![b'a'; MAX_DERIVED_BYTES / 4]);
    let budget = Budget::new(0);
    let mut ok = 0;
    for _ in 0..8 {
        let (out, _) = run(&rules, &publish("b/a", &big, &props), &budget);
        ok += out.len();
    }
    assert!(budget.bytes() <= MAX_DERIVED_BYTES, "{}", budget.bytes());
    assert_eq!(ok, 3, "three quarters and a topic each fit, not four");
}

/// `direct_dispatch`: EMQX's `union([boolean(), template()])`, default `false`. A
/// placeholder is rendered per message, and only a boolean `true` is true — a missing
/// value is the default and any other value is `false` (EMQX 6.3.1 logs
/// `bad_direct_dispatch_resolved_value` and republishes it through the rules).
#[test]
fn direct_dispatch_renders_per_message() {
    let rules = set(r#"
[rules.d]
sql = 'SELECT payload.dd AS dd FROM "d/#"'
actions = [
  { function = "republish", args = { topic = "o/default" } },
  { function = "republish", args = { topic = "o/true", direct_dispatch = true } },
  { function = "republish", args = { topic = "o/false", direct_dispatch = false } },
  { function = "republish", args = { topic = "o/text", direct_dispatch = "true" } },
  { function = "republish", args = { topic = "o/tmpl", direct_dispatch = "${dd}" } },
]
"#);
    let props = AppProperties::default();
    for (payload, tmpl) in [
        (r#"{"dd":true}"#, true),
        (r#"{"dd":false}"#, false),
        (r#"{"dd":"true"}"#, false),
        (r#"{"dd":1}"#, false),
        ("{}", false),
    ] {
        let payload = Bytes::from(payload);
        let (out, _) = run(&rules, &publish("d/1", &payload, &props), &Budget::new(0));
        let got: Vec<(&str, bool)> = out
            .iter()
            .map(|e| (republish(e).topic.as_str(), republish(e).direct_dispatch))
            .collect();
        assert_eq!(
            got,
            [
                ("o/default", false),
                ("o/true", true),
                ("o/false", false),
                ("o/text", true),
                ("o/tmpl", tmpl),
            ],
            "{payload:?}"
        );
    }
}
