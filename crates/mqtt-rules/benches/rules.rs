//! Per-message cost of the rule engine on the publish path (ADR 0083).
//!
//! Each case is one inbound publish evaluated against a loaded rule set, the work a
//! connection task does before it hands the message to the hub:
//!
//! - `no_match` — rules exist, none selects the topic: the price every publish pays
//!   once any message rule is loaded.
//! - `filter_reject` — the topic matches, `WHERE` rejects it (JSON decode + compare).
//! - `filter_republish` — the topic matches, `WHERE` passes, one republish renders.
//! - `select_star_republish` — `SELECT *` with a JSON-template republish: the most
//!   expensive common shape.
//! - `foreach_10` — a `FOREACH` over a 10-element array, one republish per element.

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use mqtt_core::AppProperties;
use mqtt_rules::{PublishInput, RuleSet};
use std::hint::black_box;

const RULES: &str = r#"
[rules.alert]
sql = '''SELECT payload.temp AS temp, clientid FROM "sensors/+/data" WHERE payload.temp > 30'''
actions = [{ function = "republish", args = { topic = "alerts/${clientid}", qos = 1, payload = "${.}" } }]

[rules.audit]
sql = '''SELECT * FROM "audit/#"'''
actions = [{ function = "republish", args = { topic = "archive/${topic}", payload = "${.}" } }]

[rules.fanout]
sql = '''FOREACH payload.readings AS r DO r.id AS id, r.v AS v FROM "batch/+"'''
actions = [{ function = "republish", args = { topic = "readings/${id}", payload = "${v}" } }]
"#;

fn run(c: &mut Criterion, name: &str, set: &RuleSet, topic: &str, payload: &Bytes) {
    let props = AppProperties::default();
    let mut out = Vec::with_capacity(16);
    c.bench_function(name, |b| {
        b.iter(|| {
            out.clear();
            let mut input = PublishInput::new("device-0042", topic, payload, 1, &props);
            input.username = Some("device-0042");
            input.node = "node-0";
            set.on_publish(black_box(&input), &mut |_, _| {}, &mut out);
            black_box(out.len());
        });
    });
}

fn bench(c: &mut Criterion) {
    let set = RuleSet::parse(RULES).expect("bench rules load").rules;
    let hot = Bytes::from_static(br#"{"temp":35.5,"humidity":40,"site":"plant-7","seq":123456}"#);
    let cold = Bytes::from_static(br#"{"temp":21.0,"humidity":40,"site":"plant-7","seq":123456}"#);
    let readings: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"id":"s{i}","v":{i}.5}}"#))
        .collect();
    let batch = Bytes::from(format!(r#"{{"readings":[{}]}}"#, readings.join(",")));
    run(c, "no_match", &set, "telemetry/device-0042", &hot);
    run(c, "filter_reject", &set, "sensors/device-0042/data", &cold);
    run(
        c,
        "filter_republish",
        &set,
        "sensors/device-0042/data",
        &hot,
    );
    run(
        c,
        "select_star_republish",
        &set,
        "audit/device-0042/login",
        &hot,
    );
    run(c, "foreach_10", &set, "batch/device-0042", &batch);
}

criterion_group!(benches, bench);
criterion_main!(benches);
