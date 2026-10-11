//! Sequences replayed through rules that keep state (ADR 0085 D11): the library half of
//! `mqttd --rule-test --sequence` (`mqttd::rule_replay`), which is what a test calls to
//! assert what a stateful rule does over several messages.
//!
//! The ADR's worked examples are here as transcripts: "hot for N seconds" with
//! `kv_held`, a quota with `kv_incr` in a window, change detection with `kv_changed`,
//! and a rule ported from EMQX that counts with `kv_store_*`. The rest pins what a
//! replay is: one store and one clock for the sequence, one dictionary per client id,
//! the rules evaluated as the broker evaluates them (every matching rule, re-entry,
//! EMQX's order), and a sequence file read within its bounds, with the line named in
//! every error.
//!
//! The binary's side (options, standard input, exit codes) is in `rules_binary.rs`;
//! `docs/RULES.md`'s transcript is run by `rules_docs.rs`.

use mqtt_rules::Functions;
use mqttd::rule_replay::{
    parse_sequence, Defaults, Replay, Report, Step, MAX_EXPIRY_EVENTS, MAX_LINE_BYTES, MAX_STEPS,
    START_MS,
};

fn steps(sequence: &str) -> Vec<Step> {
    parse_sequence(sequence.as_bytes(), &Defaults::default())
        .unwrap_or_else(|e| panic!("the test's sequence must parse: {e}"))
}

/// One statement over a sequence, as `--rule-test --sql … --sequence` runs it.
fn statement(sql: &str, sequence: &str) -> Report {
    Replay::statement(sql, &Functions::default())
        .unwrap_or_else(|e| panic!("the test's statement must compile: {e}"))
        .run(&steps(sequence))
}

/// A rules file over a sequence, as `--rule-test --sequence` runs the configured one.
fn rules(file: &str, sequence: &str) -> Report {
    Replay::rules(file, &Functions::default())
        .unwrap_or_else(|e| panic!("the test's rules must load: {e}"))
        .run(&steps(sequence))
}

/// `text` without the indentation the test's source gives it.
fn transcript(text: &str) -> String {
    let lines: Vec<&str> = text.trim_end().trim_start_matches('\n').lines().collect();
    let indent = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);
    let mut out = String::new();
    for line in lines {
        out.push_str(line.get(indent..).unwrap_or(""));
        out.push('\n');
    }
    out
}

/// The ADR's "above 30 for 5 minutes" (D3).
const HOT: &str = r#"SELECT clientid, payload.temp AS temp,
       kv_held('hot:' + clientid, payload.temp > 30, 3600) AS hot_for
FROM "sensors/+/data"
WHERE hot_for >= 300"#;

#[test]
fn hot_for_five_minutes_fires_once_the_condition_has_held_that_long() {
    let report = statement(
        HOT,
        r#"
        # a sensor that runs hot, cools, and runs hot again
        {"at": 0, "topic": "sensors/s1/data", "clientid": "s1", "payload": {"temp": 31}}
        {"at": "2m", "topic": "sensors/s1/data", "clientid": "s1", "payload": {"temp": 32}}
        {"at": "5m", "topic": "sensors/s1/data", "clientid": "s1", "payload": {"temp": 33}}
        {"after": "30s", "topic": "sensors/s1/data", "clientid": "s1", "payload": {"temp": 20}}
        {"after": "30s", "topic": "sensors/s1/data", "clientid": "s1", "payload": {"temp": 35}}
        {"at": "2h"}
        "#,
    );
    // The first hot reading is recorded on a message the WHERE rejects (the call is
    // in a SELECT field, EMQX's order); the cool reading deletes the key; the key the
    // last reading made expires an hour after it, on the sequence's clock.
    assert_eq!(
        report.text(true),
        transcript(
            r#"
            #1 +0s sensors/s1/data {"temp": 31}
              (no output)
              state rule:test: + "hot:s1" = 1735689600000 (first, expires +3600s)
            #2 +120s sensors/s1/data {"temp": 32}
              (no output)
              state rule:test: ~ "hot:s1" = 1735689600000 (first, expires +3720s)
            #3 +300s sensors/s1/data {"temp": 33}
              {"clientid":"s1","temp":33,"hot_for":300}
              state rule:test: ~ "hot:s1" = 1735689600000 (first, expires +3900s)
            #4 +330s sensors/s1/data {"temp": 20}
              (no output)
              state rule:test: - "hot:s1"
            #5 +360s sensors/s1/data {"temp": 35}
              (no output)
              state rule:test: + "hot:s1" = 1735689960000 (first, expires +3960s)
            #6 +7200s (nothing sent)
              state rule:test: - "hot:s1" (expired +3960s)
            state at the end (+7200s):
              (empty)
            "#
        )
    );
    assert_eq!(report.failed(), 0);
    assert!(report.selected());
    assert!(!report.steps[5].selected, "a pause sends nothing");
}

#[test]
fn a_quota_counts_in_a_window_and_starts_again_when_the_window_expires() {
    // "More than 3 a minute": the count's key is made with a TTL of a minute by the
    // first message and keeps that expiry (the later calls name none).
    let report = statement(
        r#"SELECT clientid,
                  CASE WHEN kv_get('q:' + clientid) = undefined
                       THEN kv_incr('q:' + clientid, 1, 60)
                       ELSE kv_incr('q:' + clientid) END AS n
           FROM "api/#" WHERE n > 3"#,
        r#"
        {"at": 0, "topic": "api/call", "clientid": "c1"}
        {"at": 10, "topic": "api/call", "clientid": "c1"}
        {"at": 20, "topic": "api/call", "clientid": "c1"}
        {"at": 30, "topic": "api/call", "clientid": "c2"}
        {"at": 40, "topic": "api/call", "clientid": "c1"}
        {"at": 59.5, "topic": "api/call", "clientid": "c1"}
        {"at": 60, "topic": "api/call", "clientid": "c1"}
        "#,
    );
    assert_eq!(
        report.text(true),
        transcript(
            r#"
            #1 +0s api/call {}
              (no output)
              state rule:test: + "q:c1" = 1 (sum, expires +60s)
            #2 +10s api/call {}
              (no output)
              state rule:test: ~ "q:c1" = 2 (sum, expires +60s)
            #3 +20s api/call {}
              (no output)
              state rule:test: ~ "q:c1" = 3 (sum, expires +60s)
            #4 +30s api/call {}
              (no output)
              state rule:test: + "q:c2" = 1 (sum, expires +90s)
            #5 +40s api/call {}
              {"clientid":"c1","n":4}
              state rule:test: ~ "q:c1" = 4 (sum, expires +60s)
            #6 +59.500s api/call {}
              {"clientid":"c1","n":5}
              state rule:test: ~ "q:c1" = 5 (sum, expires +60s)
            #7 +60s api/call {}
              (no output)
              state rule:test: - "q:c1" (expired +60s)
              state rule:test: + "q:c1" = 1 (sum, expires +120s)
            state at the end (+60s):
              rule:test: "q:c1" = 1 (sum, expires +120s)
              rule:test: "q:c2" = 1 (sum, expires +90s)
            "#
        )
    );
}

#[test]
fn change_detection_passes_only_the_messages_whose_value_differs_from_the_last() {
    // The ADR's "only when the state flips" (D3).
    let report = statement(
        r#"SELECT clientid, payload.state AS state
           FROM "machines/+/status"
           WHERE kv_changed('state:' + clientid, payload.state)"#,
        r#"
        {"topic": "machines/m1/status", "clientid": "m1", "payload": {"state": "idle"}}
        {"after": 1, "topic": "machines/m1/status", "clientid": "m1", "payload": {"state": "idle"}}
        {"after": 1, "topic": "machines/m1/status", "clientid": "m1", "payload": {"state": "running"}}
        {"after": 1, "topic": "machines/m2/status", "clientid": "m2", "payload": {"state": "running"}}
        {"after": 1, "topic": "machines/m1/status", "clientid": "m1", "payload": {"state": "running"}}
        {"after": 1, "topic": "machines/m1/status", "clientid": "m1", "payload": {"state": "idle"}}
        "#,
    );
    let outputs: Vec<&str> = report.steps.iter().map(|s| s.results[0].as_str()).collect();
    assert_eq!(
        outputs,
        [
            r#"{"clientid":"m1","state":"idle"}"#,
            "(no output)",
            r#"{"clientid":"m1","state":"running"}"#,
            r#"{"clientid":"m2","state":"running"}"#,
            "(no output)",
            r#"{"clientid":"m1","state":"idle"}"#,
        ]
    );
    // A message that changes nothing changes nothing in the store either: the write
    // names no TTL, so the expiry stays where the first write put it.
    assert_eq!(
        report.steps[0].state,
        [r#"state rule:test: + "state:m1" = "idle" (lww, expires +86400s)"#]
    );
    assert!(report.steps[1].state.is_empty());
    assert_eq!(
        report.steps[2].state,
        [r#"state rule:test: ~ "state:m1" = "running" (lww, expires +86400s)"#]
    );
}

/// A rule as EMQX has it: a counter in its `kv_store` table, republished.
const EMQX_COUNTER: &str = r#"
[rules.count]
sql = '''SELECT kv_store_put(clientid, kv_store_get(clientid, 0) + 1) AS stored,
                kv_store_get(clientid) AS n, clientid
         FROM "t/#"'''
actions = [{ function = "republish", args = { topic = "counts/${clientid}", payload = "${n}" } }]
"#;

#[test]
fn an_emqx_kv_store_rule_counts_across_the_messages_of_a_sequence() {
    let report = rules(
        EMQX_COUNTER,
        r#"
        {"topic": "t/1", "clientid": "a"}
        {"topic": "t/1", "clientid": "a"}
        {"topic": "t/2", "clientid": "b"}
        {"topic": "t/1", "clientid": "a"}
        "#,
    );
    // EMQX's table has no expiry, and one table serves every rule.
    assert_eq!(
        report.text(true),
        transcript(
            r#"
            #1 +0s t/1 {}
              count: passed
                republish counts/a qos 0 1
              state kv_store: + "a" = 1 (lww)
            #2 +0s t/1 {}
              count: passed
                republish counts/a qos 0 2
              state kv_store: ~ "a" = 2 (lww)
            #3 +0s t/2 {}
              count: passed
                republish counts/b qos 0 1
              state kv_store: + "b" = 1 (lww)
            #4 +0s t/1 {}
              count: passed
                republish counts/a qos 0 3
              state kv_store: ~ "a" = 3 (lww)
            state at the end (+0s):
              kv_store: "a" = 3 (lww)
              kv_store: "b" = 1 (lww)
            "#
        )
    );
}

#[test]
fn a_kv_store_key_of_any_type_is_shown_as_the_value_it_is() {
    let report = statement(
        r#"SELECT kv_store_put(1, 'int') AS a, kv_store_put(1.0, 'float') AS b,
                  kv_store_put('1', 'text') AS c, kv_store_put(json_decode(payload), true) AS d,
                  kv_store_put(undefined, 0) AS e
           FROM "t/#""#,
        r#"{"payload": {"b": [1, "x"], "a": 2.5}}"#,
    );
    assert_eq!(report.failed(), 0, "{}", report.text(true));
    let mut store = report.store.clone();
    store.sort();
    assert_eq!(
        store,
        [
            r#"kv_store: "1" = "text" (lww)"#,
            r#"kv_store: "undefined" = 0 (lww)"#,
            r#"kv_store: 1 = "int" (lww)"#,
            r#"kv_store: 1.0 = "float" (lww)"#,
            r#"kv_store: {"a":2.5,"b":[1,"x"]} = true (lww)"#,
        ]
    );
}

/// Three rules: one counts in a declared namespace and republishes, one reads that
/// namespace, and one runs on the republished message.
const FLEET: &str = r#"
[state.fleet]
default_ttl = "1h"

[rules.a_count]
state = "fleet"
sql = 'SELECT clientid, kv_incr(clientid) AS n FROM "reports/#"'
actions = [
  { function = "republish", args = { topic = "counted/${clientid}", payload = "${.}" } },
  { function = "console" },
]

[rules.b_reads]
state = "fleet"
sql = 'SELECT kv_get(clientid) AS seen FROM "reports/#" WHERE kv_get(clientid) >= 2'
actions = [{ function = "console" }]

[rules.c_counted]
sql = '''SELECT payload.n AS n, clientid, kv_set_once('first', payload.n) AS first FROM "counted/#"'''
actions = [{ function = "console" }]

[rules.off]
enable = false
sql = '''SELECT kv_incr('never') AS n FROM "reports/#"'''
"#;

#[test]
fn a_rules_file_runs_every_matching_rule_its_actions_and_what_they_republish() {
    let report = rules(
        FLEET,
        r#"
        {"topic": "reports/x", "clientid": "dev-1"}
        {"after": "1m", "topic": "reports/x", "clientid": "dev-1"}
        {"after": "1m", "topic": "elsewhere", "clientid": "dev-1"}
        "#,
    );
    // In the broker's order: both rules on the message (the second sees the first's
    // write, in the namespace they share), then the republished message re-enters,
    // as a message from the rule that republished it. The disabled rule never runs.
    assert_eq!(
        report.text(true),
        transcript(
            r#"
            #1 +0s reports/x {}
              a_count: passed
                republish counted/dev-1 qos 0 {"clientid":"dev-1","n":1}
                console {"clientid":"dev-1","n":1}
              b_reads: no_result
              > counted/dev-1 (republished by a_count)
                c_counted: passed
                  console {"n":1,"clientid":"a_count","first":1}
              state fleet: + "dev-1" = 1 (sum, expires +3600s)
              state rule:c_counted: + "first" = 1 (first, expires +86400s)
            #2 +60s reports/x {}
              a_count: passed
                republish counted/dev-1 qos 0 {"clientid":"dev-1","n":2}
                console {"clientid":"dev-1","n":2}
              b_reads: passed
                console {"seen":2}
              > counted/dev-1 (republished by a_count)
                c_counted: passed
                  console {"n":2,"clientid":"a_count","first":1}
              state fleet: ~ "dev-1" = 2 (sum, expires +3600s)
            #3 +120s elsewhere {}
              (no rule selects it)
            state at the end (+120s):
              fleet: "dev-1" = 2 (sum, expires +3600s)
              rule:c_counted: "first" = 1 (first, expires +86400s)
            "#
        )
    );
    assert!(report.steps[0].selected && !report.steps[2].selected);
    assert!(report.notes.is_empty());
}

#[test]
fn a_rule_that_republishes_into_its_own_from_counts_twice_and_is_stopped_there() {
    // D3: a rule runs again on its own output (EMQX's guard stops only its republish).
    let report = rules(
        r#"
[rules.loop]
sql = '''SELECT kv_incr('n') AS n FROM "loop/#"'''
actions = [{ function = "republish", args = { topic = "loop/again", payload = "${n}" } }]
"#,
        r#"{"topic": "loop/start"}"#,
    );
    assert_eq!(
        report.text(false),
        transcript(
            r#"
            #1 +0s loop/start {}
              loop: passed
                republish loop/again qos 0 1
              > loop/again (republished by loop)
                loop: passed
                  republish skipped: the message is one this rule republished
              state rule:loop: + "n" = 2 (sum, expires +86400s)
            "#
        )
    );
    assert_eq!(report.failed(), 0);
}

/// D8: a message that is sent again carries the id its publisher gave it, and
/// `kv_incr` with that id as its idempotency key counts it once. The namespace
/// remembers the id for its dedupe window (five minutes), as an entry the replay
/// shows; after that the same id is a new message.
#[test]
fn a_message_sent_again_with_the_id_it_had_is_counted_once() {
    let report = statement(
        r#"SELECT kv_incr('orders:' + clientid, 1, '1h', clientid + ':' + payload.msg) AS orders
           FROM "orders/#""#,
        r#"
        {"at": 0, "topic": "orders/new", "clientid": "c1", "qos": 1, "payload": {"msg": "a1"}}
        # the PUBACK was lost: the client sends it again
        {"at": 2, "topic": "orders/new", "clientid": "c1", "qos": 1, "payload": {"msg": "a1"}}
        {"at": 10, "topic": "orders/new", "clientid": "c1", "qos": 1, "payload": {"msg": "a2"}}
        # another client's a1 is another message
        {"at": 11, "topic": "orders/new", "clientid": "c2", "qos": 1, "payload": {"msg": "a1"}}
        {"at": "5m", "topic": "orders/new", "clientid": "c1", "qos": 1, "payload": {"msg": "a1"}}
        "#,
    );
    assert_eq!(
        report.text(true),
        transcript(
            r#"
            #1 +0s orders/new {"msg": "a1"}
              {"orders":1}
              state rule:test: + idempotency key "c1:a1" = 1735689600000 (first, expires +300s)
              state rule:test: + "orders:c1" = 1 (sum, expires +3600s)
            #2 +2s orders/new {"msg": "a1"}
              {"orders":1}
            #3 +10s orders/new {"msg": "a2"}
              {"orders":2}
              state rule:test: + idempotency key "c1:a2" = 1735689610000 (first, expires +310s)
              state rule:test: ~ "orders:c1" = 2 (sum, expires +3610s)
            #4 +11s orders/new {"msg": "a1"}
              {"orders":1}
              state rule:test: + idempotency key "c2:a1" = 1735689611000 (first, expires +311s)
              state rule:test: + "orders:c2" = 1 (sum, expires +3611s)
            #5 +300s orders/new {"msg": "a1"}
              {"orders":3}
              state rule:test: - idempotency key "c1:a1" (expired +300s)
              state rule:test: + idempotency key "c1:a1" = 1735689900000 (first, expires +600s)
              state rule:test: ~ "orders:c1" = 3 (sum, expires +3900s)
            state at the end (+300s):
              rule:test: idempotency key "c1:a1" = 1735689900000 (first, expires +600s)
              rule:test: idempotency key "c1:a2" = 1735689610000 (first, expires +310s)
              rule:test: idempotency key "c2:a1" = 1735689611000 (first, expires +311s)
              rule:test: "orders:c1" = 3 (sum, expires +3900s)
              rule:test: "orders:c2" = 1 (sum, expires +3611s)
            "#
        )
    );
}

/// D3 said a rule that counts and republishes into its own `FROM` counts twice. With
/// the message's id as the idempotency key, and the id carried in what it
/// republishes, it counts once.
#[test]
fn a_rule_that_re_enters_itself_counts_once_with_an_idempotency_key() {
    let report = rules(
        r#"
[rules.loop]
sql = '''SELECT payload.msg AS msg, kv_incr('n', 1, 60, payload.msg) AS n FROM "loop/#"'''
actions = [{ function = "republish", args = { topic = "loop/again", payload = "${.}" } }]
"#,
        r#"{"topic": "loop/start", "payload": {"msg": "a1"}}"#,
    );
    assert_eq!(
        report.text(false),
        transcript(
            r#"
            #1 +0s loop/start {"msg": "a1"}
              loop: passed
                republish loop/again qos 0 {"msg":"a1","n":1}
              > loop/again (republished by loop)
                loop: passed
                  republish skipped: the message is one this rule republished
              state rule:loop: + idempotency key "a1" = 1735689600000 (first, expires +300s)
              state rule:loop: + "n" = 1 (sum, expires +60s)
            "#
        )
    );
    assert_eq!(report.failed(), 0);
}

#[test]
fn the_clock_is_the_sequences_and_a_replay_prints_the_same_every_time() {
    let sql = r#"SELECT now_timestamp() AS s, now_timestamp('millisecond') AS ms,
                        rfc3339_to_unix_ts(now_rfc3339()) AS text, timestamp, publish_received_at, id,
                        format_date('second', 0, '%Y-%m-%d %H:%M:%S') AS date
                 FROM "t/#""#;
    let sequence = r#"
        {"topic": "t/1"}
        {"at": 1.25, "topic": "t/1"}
        {"at": "1d", "topic": "t/1"}
        "#;
    let report = statement(sql, sequence);
    assert_eq!(START_MS, 1_735_689_600_000, "2025-01-01T00:00:00Z");
    let outputs: Vec<&str> = report.steps.iter().map(|s| s.results[0].as_str()).collect();
    assert_eq!(
        outputs,
        [
            r#"{"s":1735689600,"ms":1735689600000,"text":1735689600,"timestamp":1735689600000,"publish_received_at":1735689600000,"id":"00000000000000000000000000000001","date":"2025-01-01 00:00:00"}"#,
            r#"{"s":1735689601,"ms":1735689601250,"text":1735689601,"timestamp":1735689601250,"publish_received_at":1735689601250,"id":"00000000000000000000000000000002","date":"2025-01-01 00:00:01"}"#,
            r#"{"s":1735776000,"ms":1735776000000,"text":1735776000,"timestamp":1735776000000,"publish_received_at":1735776000000,"id":"00000000000000000000000000000003","date":"2025-01-02 00:00:00"}"#,
        ]
    );
    assert_eq!(
        report.text(true),
        statement(sql, sequence).text(true),
        "nothing in a replay reads the wall clock or draws a random id"
    );
}

#[test]
fn a_key_expires_when_the_sequences_clock_passes_its_ttl() {
    let report = statement(
        r#"SELECT kv_get('k') AS before, kv_ttl('k') AS left,
                  CASE WHEN payload.set = true THEN kv_set('k', 'v', 10) END AS set
           FROM "t/#""#,
        r#"
        {"at": 0, "payload": {"set": true}}
        {"at": 9.999}
        {"at": 9.999, "topic": "t/1"}
        {"at": 10, "topic": "t/1"}
        {"at": 11, "topic": "t/1"}
        "#,
    );
    assert_eq!(
        report.text(true),
        transcript(
            r#"
            #1 +0s t/1 {"set": true}
              {"before":"undefined","left":"undefined","set":"v"}
              state rule:test: + "k" = "v" (lww, expires +10s)
            #2 +9.999s (nothing sent)
            #3 +9.999s t/1 {}
              {"before":"v","left":1,"set":"undefined"}
            #4 +10s t/1 {}
              {"before":"undefined","left":"undefined","set":"undefined"}
              state rule:test: - "k" (expired +10s)
            #5 +11s t/1 {}
              {"before":"undefined","left":"undefined","set":"undefined"}
            state at the end (+11s):
              (empty)
            "#
        )
    );
}

#[test]
fn a_dictionary_is_one_client_ids_for_the_whole_sequence() {
    // In the broker a dictionary is a connection's; in a sequence, a client id's. An
    // event of the client and its publishes share it, as they do on its connection.
    let report = rules(
        r#"
[rules.on_connect]
sql = '''SELECT proc_dict_put('since', timestamp) AS before FROM "$events/client/connected"'''
actions = [{ function = "console" }]

[rules.on_publish]
sql = '''SELECT clientid, proc_dict_get('since') AS since, proc_dict_put('n', payload.n) AS last FROM "t/#"'''
actions = [{ function = "console" }]
"#,
        r#"
        {"at": 5, "event": "client.connected", "clientid": "a"}
        {"at": 6, "topic": "t/1", "clientid": "a", "payload": {"n": 1}}
        {"at": 7, "topic": "t/1", "clientid": "b", "payload": {"n": 2}}
        {"at": 8, "topic": "t/1", "clientid": "a", "payload": {"n": 3}}
        "#,
    );
    assert_eq!(
        report.text(true),
        transcript(
            r#"
            #1 +5s event client.connected (a)
              on_connect: passed
                console {"before":"undefined"}
            #2 +6s t/1 {"n": 1}
              on_publish: passed
                console {"clientid":"a","since":1735689605000,"last":"undefined"}
            #3 +7s t/1 {"n": 2}
              on_publish: passed
                console {"clientid":"b","since":"undefined","last":"undefined"}
            #4 +8s t/1 {"n": 3}
              on_publish: passed
                console {"clientid":"a","since":1735689605000,"last":1}
            state at the end (+8s):
              (empty)
            "#
        )
    );
}

#[test]
fn a_statement_on_events_runs_against_samples_at_the_sequences_time() {
    // As `--rule-test`: a statement that selects only events gets the first one it
    // names unless the step names another.
    let report = statement(
        r#"SELECT event, clientid, timestamp, connected_at, disconnected_at,
                  kv_incr(clientid) AS n
           FROM "$events/client/connected", "$events/client/disconnected""#,
        r#"
        {"at": 1, "clientid": "a", "topic": "t/1"}
        {"at": 3, "clientid": "a", "event": "client.disconnected"}
        {"at": 4, "clientid": "a", "event": "session.subscribed"}
        "#,
    );
    assert_eq!(
        report.text(false),
        transcript(
            r#"
            #1 +1s event client.connected (a)
              {"event":"client.connected","clientid":"a","timestamp":1735689601000,"connected_at":1735689601000,"disconnected_at":"undefined","n":1}
              state rule:test: + "a" = 1 (sum, expires +86401s)
            #2 +3s event client.disconnected (a)
              {"event":"client.disconnected","clientid":"a","timestamp":1735689603000,"connected_at":1735689602000,"disconnected_at":1735689603000,"n":2}
              state rule:test: ~ "a" = 2 (sum, expires +86401s)
            #3 +4s event session.subscribed (a)
              (not selected: the statement's FROM does not select the session.subscribed event)
            "#
        )
    );
    assert_eq!(report.failed(), 0);
}

#[test]
fn a_step_that_fails_is_reported_and_the_replay_goes_on_with_what_it_wrote() {
    // The count is a SELECT field before the one that fails: the write stays (D3).
    let report = statement(
        r#"SELECT kv_incr('n') AS n, payload.temp + 1 AS next FROM "t/#""#,
        r#"
        {"topic": "t/1", "payload": {"temp": 1}}
        {"topic": "t/1", "payload": "not json"}
        {"topic": "t/1", "payload": {"temp": 3}}
        {"topic": "other"}
        "#,
    );
    assert_eq!(
        report.text(false),
        transcript(
            r#"
            #1 +0s t/1 {"temp": 1}
              {"n":1,"next":2}
              state rule:test: + "n" = 1 (sum, expires +86400s)
            #2 +0s t/1 not json
              FAILED: payload is not JSON, so payload.<field> is unreadable (invalid JSON: expected ident at line 1 column 2)
              state rule:test: ~ "n" = 2 (sum, expires +86400s)
            #3 +0s t/1 {"temp": 3}
              {"n":3,"next":4}
              state rule:test: ~ "n" = 3 (sum, expires +86400s)
            #4 +0s other {}
              (not selected: topic "other" matches none of the FROM filters (t/#))
            "#
        )
    );
    assert_eq!(report.failed(), 1);
    assert!(report.steps[1].failed && !report.steps[2].failed && !report.steps[3].failed);
    assert!(!report.steps[3].selected);
}

#[test]
fn a_failing_rule_or_action_of_a_rules_file_is_reported_with_its_reason() {
    let report = rules(
        r#"
[rules.a_bad_sql]
sql = '''SELECT kv_set('k', 1) AS set, kv_incr('k') AS n FROM "t/#"'''

[rules.b_bad_action]
sql = 'SELECT payload.to AS to FROM "t/#"'
actions = [{ function = "republish", args = { topic = "${to}", payload = "x" } }]
"#,
        r#"
        {"topic": "t/1", "payload": {"to": "a/+"}}
        {"topic": "t/1", "payload": {"to": "a/b"}}
        "#,
    );
    let text = report.text(false);
    assert_eq!(report.failed(), 2, "{text}");
    let first = &report.steps[0].results;
    assert!(
        first[0].starts_with("a_bad_sql: FAILED: kv_incr(): wrong_type"),
        "{text}"
    );
    assert_eq!(first[1], "b_bad_action: passed", "{text}");
    assert!(first[2].starts_with("  action FAILED: "), "{text}");
    // The write before the failing call stays, and is shown.
    assert_eq!(
        report.steps[0].state,
        [r#"state rule:a_bad_sql: + "k" = 1 (lww, expires +86400s)"#]
    );
    assert_eq!(
        report.steps[1].results[1..],
        ["b_bad_action: passed", "  republish a/b qos 0 x"]
    );
}

#[test]
fn nothing_selected_is_told_apart_from_no_output() {
    let report = statement(r#"SELECT 1 AS one FROM "a/#""#, r#"{"topic": "b/1"}"#);
    assert!(!report.selected());
    assert_eq!(report.failed(), 0);
    let report = statement(
        r#"SELECT 1 AS one FROM "a/#" WHERE false"#,
        r#"{"topic": "a/1"}"#,
    );
    assert!(report.selected());
    assert_eq!(report.steps[0].results, ["(no output)"]);
}

#[test]
fn a_replay_never_touches_the_store_its_functions_carried() {
    // D11: no test touches the running store. The functions a replay is given carry
    // the broker's store (as `functions_from_config` hands it over); the replay keeps
    // its state in one of its own, under the same ceiling.
    let (functions, running) =
        mqttd::rules::functions_and_state_from_config(&mqtt_config::Rules::default());
    let running = running.expect("rule state is on by default");
    let sequence = steps(r#"{"topic": "t/1"}"#);
    let report = Replay::statement(r#"SELECT kv_incr('n') AS n FROM "t/#""#, &functions)
        .expect("compiles")
        .run(&sequence);
    assert_eq!(report.steps[0].results, [r#"{"n":1}"#]);
    let report = Replay::rules(EMQX_COUNTER, &functions)
        .expect("loads")
        .run(&sequence);
    assert_eq!(report.store, [r#"kv_store: "test-client" = 1 (lww)"#]);
    assert!(running.namespaces().is_empty());
    assert_eq!(running.bytes(), 0);
    assert!(running.held().is_empty());
}

#[test]
fn where_rule_state_is_switched_off_a_replay_of_a_stateful_rule_does_not_load() {
    let off = Functions::default().without_state();
    let e =
        Replay::statement(r#"SELECT kv_incr('n') AS n FROM "t/#""#, &off).expect_err("no store");
    assert!(e.contains("rules.state_max_bytes"), "{e}");
    let e = Replay::rules(EMQX_COUNTER, &off).expect_err("no store");
    assert!(e.contains("rules.state_max_bytes"), "{e}");
    // A statement that keeps no state replays all the same.
    let report = Replay::statement(r#"SELECT topic FROM "t/#""#, &off)
        .expect("compiles")
        .run(&steps(r#"{"topic": "t/1"}"#));
    assert_eq!(
        report.text(true),
        transcript(
            r#"
        #1 +0s t/1 {}
          {"topic":"t/1"}
        state at the end (+0s):
          (empty)
        "#
        )
    );
}

#[test]
fn a_sequence_line_gives_the_message_and_what_it_leaves_out_is_the_defaults() {
    let defaults = Defaults {
        topic: "d/topic".to_string(),
        clientid: "d-client".to_string(),
        username: Some("d-user".to_string()),
        qos: 1,
        ..Defaults::default()
    };
    let sequence = concat!(
        "\n",
        "# a comment\r\n",
        r#"{"payload": "text \"quoted\"\n"}"#,
        "\n   \n",
        r#"  {"after": "250ms", "topic": "a/b", "payload": {"k":  [1, 2.50]}, "qos": 2, "retain": true, "clientid": "c", "username": "u"}  "#,
        "\n",
        r#"{"after": "1h", "event": "client/connected"}"#,
        "\n",
        r#"{"after": 0.5}"#,
    );
    let steps = parse_sequence(sequence.as_bytes(), &defaults).expect("parses");
    assert_eq!(steps.len(), 4);
    assert_eq!(
        steps.iter().map(|s| s.line).collect::<Vec<_>>(),
        [3, 5, 6, 7]
    );
    assert_eq!(
        steps.iter().map(|s| s.at_ms).collect::<Vec<_>>(),
        [0, 250, 3_600_250, 3_600_750]
    );
    let first = steps[0].send.as_ref().expect("a publish");
    // A JSON string is those bytes.
    assert_eq!(&first.payload[..], b"text \"quoted\"\n");
    assert_eq!(
        (&*first.topic, &*first.clientid, first.username.as_deref()),
        ("d/topic", "d-client", Some("d-user"))
    );
    assert_eq!((first.qos, first.retain, first.event), (1, false, None));
    let second = steps[1].send.as_ref().expect("a publish");
    // Any other JSON value is its text as the line has it.
    assert_eq!(&second.payload[..], br#"{"k":  [1, 2.50]}"#);
    assert_eq!(
        (
            &*second.topic,
            &*second.clientid,
            second.username.as_deref()
        ),
        ("a/b", "c", Some("u"))
    );
    assert_eq!((second.qos, second.retain), (2, true));
    let third = steps[2].send.as_ref().expect("an event");
    assert_eq!(third.event, Some(mqtt_rules::EventKind::ClientConnected));
    assert_eq!(&third.payload[..], b"{}");
    assert!(steps[3].send.is_none(), "only moves the clock");
}

#[test]
fn a_sequence_that_cannot_be_read_names_the_line_and_the_reason() {
    let refused = |sequence: &str| {
        parse_sequence(sequence.as_bytes(), &Defaults::default())
            .expect_err(&format!("must be refused: {sequence}"))
    };
    for (sequence, line, reason) in [
        ("{\"topic\": \"a\"}\n{not json}", "line 2: ", "not a step"),
        ("\n\n[1]", "line 3: ", "not a step"),
        (r#"{"tpoic": "a"}"#, "line 1: ", "unknown field `tpoic`"),
        (
            r#"{"at": 1, "after": 1}"#,
            "line 1: ",
            "`at` or `after`, not both",
        ),
        (
            "{\"at\": 10}\n# later\n{\"at\": \"9s\", \"topic\": \"a\"}",
            "line 3: ",
            "is before the step before it (+10s)",
        ),
        (r#"{"at": -1}"#, "line 1: ", "`at` is a number of seconds"),
        (
            r#"{"at": "soon"}"#,
            "line 1: ",
            "`at` is a number of seconds",
        ),
        (
            r#"{"at": "5 m"}"#,
            "line 1: ",
            "`at` is a number of seconds",
        ),
        (
            r#"{"after": true}"#,
            "line 1: ",
            "`after` is a number of seconds",
        ),
        (
            r#"{"at": "101000d"}"#,
            "line 1: ",
            "further than a sequence may run",
        ),
        (
            "{\"at\": \"36000d\"}\n{\"after\": \"36000d\"}",
            "line 2: ",
            "further than it may run",
        ),
        (
            r#"{"topic": "a", "qos": 3}"#,
            "line 1: ",
            "`qos` is 0, 1 or 2, not 3",
        ),
        (r#"{"topic": "a/#"}"#, "line 1: ", "not a valid topic name"),
        (r#"{"topic": ""}"#, "line 1: ", "not a valid topic name"),
        (
            r#"{"topic": "a", "clientid": ""}"#,
            "line 1: ",
            "`clientid` is empty",
        ),
        (
            r#"{"event": "client.nope"}"#,
            "line 1: ",
            "`event` is one of client.connected",
        ),
        (r#"{"at": 1, "qos": 1}"#, "line 1: ", "only moves the clock"),
        ("{\"topic\": \"a\"}\n\u{feff}{}", "line 2: ", "not a step"),
    ] {
        let e = refused(sequence);
        assert!(
            e.starts_with(line) && e.contains(reason),
            "{sequence}: got {e:?}"
        );
        assert!(!e.contains("at line 1 column"), "serde's own line: {e:?}");
    }
    assert!(refused("").contains("has no step"));
    assert!(refused("# only a comment\n\n").contains("has no step"));
    let e = parse_sequence(&b"{\"topic\": \"a\"}\n\xff\xfe\n"[..], &Defaults::default())
        .expect_err("not UTF-8");
    assert_eq!(e, "line 2: not UTF-8");
}

#[test]
fn a_sequence_is_bounded_in_steps_and_in_the_length_of_a_line() {
    let parse = |sequence: &str| parse_sequence(sequence.as_bytes(), &Defaults::default());
    // The most steps, and one more.
    let most = "{\"topic\": \"a\"}\n".repeat(MAX_STEPS);
    assert_eq!(parse(&most).expect("the most steps").len(), MAX_STEPS);
    let e = parse(&format!("{most}# a comment is not a step\n{{}}")).expect_err("one more");
    assert_eq!(
        e,
        format!(
            "line {}: a sequence has at most {MAX_STEPS} steps",
            MAX_STEPS + 2
        )
    );
    // The longest line, with either line ending, and one byte more.
    let wrapper = r#"{"topic": "a", "payload": ""}"#.len();
    let line = |len: usize| {
        format!(
            r#"{{"topic": "a", "payload": "{}"}}"#,
            "x".repeat(len - wrapper)
        )
    };
    let longest = line(MAX_LINE_BYTES);
    assert_eq!(longest.len(), MAX_LINE_BYTES);
    for ending in ["", "\n", "\r\n"] {
        let steps = parse(&format!("{{}}\n{longest}{ending}")).expect("the longest line");
        let payload = &steps[1].send.as_ref().expect("a publish").payload;
        assert_eq!(payload.len(), MAX_LINE_BYTES - wrapper);
    }
    for ending in ["", "\n", "\r\n"] {
        let e = parse(&format!("{{}}\n{}{ending}{{}}\n", line(MAX_LINE_BYTES + 1)))
            .expect_err("a byte too long");
        assert_eq!(
            e,
            format!("line 2: longer than the {MAX_LINE_BYTES} bytes a line of a sequence may be")
        );
    }
}

#[test]
fn what_a_payload_or_a_key_prints_cannot_break_the_reports_lines() {
    let report = statement(
        r#"SELECT kv_set(payload, 1) AS set, payload AS p FROM "t/#""#,
        r#"{"topic": "t/1", "payload": "a\nb\u001b[2Jc"}"#,
    );
    let text = report.text(true);
    assert!(!text.contains('\u{1b}'), "{text:?}");
    assert_eq!(
        text,
        transcript(
            r#"
            #1 +0s t/1 a\nb\u{1b}[2Jc
              {"set":1,"p":"a\nb\u001B[2Jc"}
              state rule:test: + "a\nb\u001b[2Jc" = 1 (lww, expires +86400s)
            state at the end (+0s):
              rule:test: "a\nb\u001b[2Jc" = 1 (lww, expires +86400s)
            "#
        )
    );
}

/// ADR 0085 T3: an `eventual` namespace loads, and a replay, which is one node, shows
/// of it what it shows of a `local` one, with a note that a cluster would replicate it.
/// A deleted key is gone from what the replay shows, as in a `local` namespace.
#[test]
fn an_eventual_namespace_replays_as_a_local_one_with_a_note() {
    let file = |mode: &str| {
        format!(
            "[state.fleet]\nconsistency = \"{mode}\"\n[rules.count]\nstate = \"fleet\"\n\
             sql = '''SELECT kv_incr(clientid, 1, 3600) AS n, kv_changed('s', payload.s) AS c, \
             kv_add('seen', clientid) AS seen, kv_set('gone', 1) AS g, kv_del('gone') AS d \
             FROM \"reports/#\"'''\nactions = [{{ function = \"console\" }}]\n"
        )
    };
    let sequence = r#"
        {"at": 0, "topic": "reports/x", "clientid": "dev-1", "payload": {"s": "on"}}
        {"at": "1m", "topic": "reports/x", "clientid": "dev-2", "payload": {"s": "on"}}
        {"at": "2h", "topic": "reports/x", "clientid": "dev-1", "payload": {"s": "off"}}
    "#;
    let local = rules(&file("local"), sequence);
    let eventual = rules(&file("eventual"), sequence);
    assert!(local.notes.is_empty());
    assert_eq!(
        eventual.notes,
        [
            "namespace `fleet` is replicated in a cluster; a replay is one node, where it \
          behaves as consistency = \"local\""
        ]
    );
    assert_eq!(eventual.failed(), 0);
    assert_eq!(
        format!("{:?}", eventual.steps),
        format!("{:?}", local.steps)
    );
    assert_eq!(eventual.store, local.store);
    assert_eq!(eventual.store.len(), 3, "{:?}", eventual.store);
}

/// "No message for ten minutes" (ADR 0085 D7): one rule refreshes a key with a TTL on
/// every reading, another selects the key's expiry.
const SILENT: &str = r#"
[state.seen]
default_ttl = "10m"

[rules.a_seen]
state = "seen"
sql = '''SELECT kv_set(clientid, payload.temp, 600) AS last FROM "sensors/+/data"'''

[rules.b_silent]
sql = '''SELECT key AS device, value AS last_temp, timestamp - expired_at AS late
         FROM "$events/state/expired" WHERE namespace = 'seen' '''
actions = [{ function = "republish", args = { topic = "alerts/silent/${device}", payload = "${.}" } }]

[rules.c_alerts]
sql = '''SELECT kv_incr('alerts') AS n FROM "alerts/silent/#"'''

[rules.d_emqx_events]
sql = '''SELECT event FROM "$events/#"'''
actions = [{ function = "console" }]
"#;

/// D7 and D11: an expiry event fires in the replay, per key, at the key's expiry
/// time and before the step's own message, in order of expiry; what its rules
/// republish re-enters the rules; and a rule on `"$events/#"` does not see it.
#[test]
fn silent_for_ten_minutes_raises_the_expiry_event_at_each_keys_expiry_time() {
    let report = rules(
        SILENT,
        r#"
        {"topic": "sensors/d1/data", "clientid": "d1", "payload": {"temp": 20}}
        {"at": "1m", "topic": "sensors/d2/data", "clientid": "d2", "payload": {"temp": 21}}
        {"at": "5m", "topic": "sensors/d1/data", "clientid": "d1", "payload": {"temp": 22}}
        {"at": "20m", "topic": "sensors/d1/data", "clientid": "d1", "payload": {"temp": 23}}
        "#,
    );
    assert_eq!(
        report.text(true),
        transcript(
            r#"
            #1 +0s sensors/d1/data {"temp": 20}
              a_seen: passed
              state seen: + "d1" = 20 (lww, expires +600s)
            #2 +60s sensors/d2/data {"temp": 21}
              a_seen: passed
              state seen: + "d2" = 21 (lww, expires +660s)
            #3 +300s sensors/d1/data {"temp": 22}
              a_seen: passed
              state seen: ~ "d1" = 22 (lww, expires +900s)
            #4 +1200s sensors/d1/data {"temp": 23}
              event state.expired seen: "d2" at +660s
                b_silent: passed
                  republish alerts/silent/d2 qos 0 {"device":"d2","last_temp":21,"late":0}
                > alerts/silent/d2 (republished by b_silent)
                  c_alerts: passed
              event state.expired seen: "d1" at +900s
                b_silent: passed
                  republish alerts/silent/d1 qos 0 {"device":"d1","last_temp":22,"late":0}
                > alerts/silent/d1 (republished by b_silent)
                  c_alerts: passed
              a_seen: passed
              state seen: - "d2" (expired +660s)
              state rule:c_alerts: + "alerts" = 1 (sum, expires +87060s)
              state seen: - "d1" (expired +900s)
              state rule:c_alerts: ~ "alerts" = 2 (sum, expires +87060s)
              state seen: + "d1" = 23 (lww, expires +1800s)
            state at the end (+1200s):
              rule:c_alerts: "alerts" = 2 (sum, expires +87060s)
              seen: "d1" = 23 (lww, expires +1800s)
            "#
        )
    );
    assert_eq!(report.failed(), 0);

    // The same file with the namespace replicated: its keys expire, and the event is
    // not raised for them yet (which member raises it is not built). A note says so.
    let replicated = rules(
        &SILENT.replace("[state.seen]", "[state.seen]\nconsistency = \"eventual\""),
        "{\"topic\": \"sensors/d1/data\", \"clientid\": \"d1\", \"payload\": {\"temp\": 20}}\n\
         {\"at\": \"20m\"}\n",
    );
    assert_eq!(
        replicated.notes,
        [
            "namespace `seen` is replicated in a cluster; a replay is one node, where it \
             behaves as consistency = \"local\"",
            "namespace `seen` is replicated: the expiry of its keys raises no \
             $events/state/expired yet, here or in a broker"
        ]
    );
    assert_eq!(
        replicated.steps[1].state,
        [r#"state seen: - "d1" (expired +600s)"#]
    );
    assert!(replicated.steps[1].results.is_empty());
}

/// D7: a rule on the event may write state, the key that expired included. In a
/// replay what it writes expires in turn within the same step, each at its own time;
/// a step raises at most `MAX_EXPIRY_EVENTS`, and says so.
#[test]
fn a_rule_on_the_expiry_event_may_write_the_key_again_and_a_step_bounds_the_events() {
    let sql = r#"SELECT CASE WHEN event = 'state.expired' THEN kv_set(key, value + 1, 1)
                            ELSE kv_set('k', 0, 1) END AS n,
                        now_timestamp() AS at
                 FROM "t/#", "$events/state/expired""#;
    let report = statement(
        sql,
        r#"
        {"topic": "t/1"}
        {"at": 3}
        "#,
    );
    assert_eq!(
        report.text(true),
        transcript(
            r#"
            #1 +0s t/1 {}
              {"n":0,"at":1735689600}
              state rule:test: + "k" = 0 (lww, expires +1s)
            #2 +3s (nothing sent)
              event state.expired rule:test: "k" at +1s
                {"n":1,"at":1735689601}
              event state.expired rule:test: "k" at +2s
                {"n":2,"at":1735689602}
              event state.expired rule:test: "k" at +3s
                {"n":3,"at":1735689603}
              state rule:test: - "k" (expired +1s)
              state rule:test: + "k" = 1 (lww, expires +2s)
              state rule:test: - "k" (expired +2s)
              state rule:test: + "k" = 2 (lww, expires +3s)
              state rule:test: - "k" (expired +3s)
              state rule:test: + "k" = 3 (lww, expires +4s)
            state at the end (+3s):
              rule:test: "k" = 3 (lww, expires +4s)
            "#
        )
    );
    assert!(report.steps[1].selected, "the event's rules ran");

    let long = statement(sql, "{\"topic\": \"t/1\"}\n{\"at\": \"5h\"}\n");
    let events = |prefix: &str| {
        long.steps[1]
            .results
            .iter()
            .filter(|line| line.starts_with(prefix))
            .count()
    };
    assert_eq!(events("event state.expired"), MAX_EXPIRY_EVENTS);
    assert_eq!(
        events("(more keys expired: a step of a replay raises at most 10000 expiry events)"),
        1
    );
    assert!(
        long.store.is_empty(),
        "the last key expired and was not written again"
    );
}

/// D7: without a rule on the event a replay reports the expired keys and raises
/// nothing, also where a rule selects every EMQX event with `"$events/#"`.
#[test]
fn a_wildcard_on_every_event_is_not_sent_the_expiry_event() {
    let report = rules(
        r#"
        [rules.a_set]
        sql = '''SELECT kv_set('k', 1, 10) AS v FROM "t/#"'''

        [rules.b_events]
        sql = '''SELECT event FROM "$events/#"'''
        actions = [{ function = "console" }]
        "#,
        r#"
        {"topic": "t/1"}
        {"at": 20}
        {"at": 21, "event": "client.connected"}
        "#,
    );
    assert_eq!(
        report.text(true),
        transcript(
            r#"
            #1 +0s t/1 {}
              a_set: passed
              state rule:a_set: + "k" = 1 (lww, expires +10s)
            #2 +20s (nothing sent)
              state rule:a_set: - "k" (expired +10s)
            #3 +21s event client.connected (test-client)
              b_events: passed
                console {"event":"client.connected"}
            state at the end (+21s):
              (empty)
            "#
        )
    );
}

/// A step may send a sample of the event itself, as `--rule-test --event` builds
/// it: a key named by the topic, holding the payload.
#[test]
fn a_step_can_send_a_sample_of_the_expiry_event() {
    let report = statement(
        r#"SELECT namespace, key, value FROM "$events/state/expired""#,
        r#"{"event": "state.expired", "topic": "seen:d1", "payload": "gone"}"#,
    );
    assert_eq!(
        report.text(false),
        transcript(
            r#"
            #1 +0s event state.expired (test-client)
              {"namespace":"sample","key":"seen:d1","value":"gone"}
            "#
        )
    );
}
