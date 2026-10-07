#![no_main]
//! Fuzz target: evaluating rules over an arbitrary payload must never panic or hang.
//!
//! The payload, its topic and its user properties are publisher-controlled, and rules
//! run on every publish they select (ADR 0083). These rules reach into the payload
//! every way a rule can — JSON paths, indices and ranges, FOREACH, regexes whose
//! pattern comes from the payload, conversions, templates — so the evaluator's error
//! paths, not just its happy ones, are exercised. A rule failing is a correct answer;
//! a panic is a finding.

use libfuzzer_sys::fuzz_target;

const RULES: &str = r##"
[rules.paths]
sql = '''SELECT payload.a[1] AS f, payload.a[-1].b AS l, payload.a[2..-1] AS s, payload.x * 2 + 1 AS d FROM "#" WHERE payload.x > 0 OR is_null(payload.x)'''
actions = [{ function = "republish", args = { topic = "o/${f}", qos = "${qos}", payload = "${.}" } }]

[rules.each]
sql = '''FOREACH payload.a AS e DO e, str(e) AS s, int(e) AS i INCASE is_num(e) FROM "#"'''
actions = [{ function = "republish", args = { topic = "e/${s}", payload = "${i}" } }]

[rules.funcs]
sql = '''SELECT regex_match(str(payload.p), str(payload.r)) AS m, regex_replace(str(payload), '\d+', 'N') AS n, split(str(payload), ',') AS parts, base64_decode(str(payload.b)) AS raw, json_decode(str(payload.j)) AS j, map_get(str(payload.k), payload) AS v, substr(str(payload), 1, 4) AS sub, format_date('second', 'Z', str(payload.fmt), 0) AS fd, sprintf(str(payload.fmt), 1) AS sp FROM "#"'''
actions = [{ function = "console" }]

[rules.star]
sql = '''SELECT * FROM "#" WHERE topic =~ 'a/+' '''
actions = [{ function = "republish", args = { topic = "${topic}/x", user_properties = "${pub_props.'User-Property'}" } }]
"##;

fuzz_target!(|data: &[u8]| {
    static SET: std::sync::OnceLock<mqtt_rules::RuleSet> = std::sync::OnceLock::new();
    let set = SET.get_or_init(|| {
        mqtt_rules::RuleSet::parse(RULES)
            .expect("fuzz rules load")
            .rules
    });
    // The first byte picks the topic; the rest is the payload, sent raw and as a user
    // property value.
    let (topic, payload) = match data.split_first() {
        Some((b, rest)) if b % 2 == 0 => ("a/b", rest),
        Some((_, rest)) => ("t/x", rest),
        None => ("a/b", data),
    };
    let payload = bytes::Bytes::copy_from_slice(payload);
    let props = mqtt_core::AppProperties {
        user_properties: vec![("k".into(), String::from_utf8_lossy(&payload).into_owned())],
        ..Default::default()
    };
    let input = mqtt_rules::PublishInput::new("fuzz", topic, &payload, 1, &props);
    let mut out = Vec::new();
    set.on_publish(&input, &mut |_, _| {}, &mut out);
});
