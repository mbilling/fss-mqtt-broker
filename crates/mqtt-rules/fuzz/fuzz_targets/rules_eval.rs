#![no_main]
//! Fuzz target: evaluating rules over an arbitrary payload must never panic or hang.
//!
//! The payload, its topic and its user properties are publisher-controlled, and rules
//! run on every publish they select (ADR 0083). These rules reach into the payload
//! every way a rule can — JSON paths, indices and ranges, FOREACH, regexes whose
//! pattern comes from the payload, conversions, templates, and every argument that
//! sizes, nests or dates what a function builds (pad lengths, decimals, replacements,
//! separators, key paths, ranges, timestamps, date formats) — so the evaluator's error
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

# One rule per function, so one failing does not stop the others from running.
[rules.size_pad]
sql = '''SELECT pad(str(payload.s), payload.n, str(payload.dir), str(payload.c)) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/pad", payload = "${v}" } }]

[rules.size_float2str]
sql = '''SELECT float2str(payload.v, payload.d) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/float2str", payload = "${v}" } }]

[rules.size_float]
sql = '''SELECT float(payload.v, payload.d) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/float", payload = "${v}" } }]

[rules.size_replace]
sql = '''SELECT replace(str(payload.s), str(payload.c), str(payload.r)) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/replace", payload = "${v}" } }]

[rules.size_regex_replace]
sql = '''SELECT regex_replace(str(payload.s), str(payload.re), str(payload.r)) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/regex_replace", payload = "${v}" } }]

[rules.size_join]
sql = '''SELECT join_to_string(str(payload.c), payload.a) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/join", payload = "${v}" } }]

[rules.size_map_put]
sql = '''SELECT map_put(str(payload.k), payload.v, payload) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/map_put", payload = "${v}" } }]

[rules.size_mput]
sql = '''SELECT mput(payload.a, 1, map_new()) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/mput", payload = "${v}" } }]

[rules.size_map_to_range]
sql = '''SELECT map_to_range(payload.n, payload.lo, payload.hi) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/map_to_range", payload = "${v}" } }]

[rules.size_hash_to_range]
sql = '''SELECT hash_to_range(str(payload.s), payload.lo, payload.hi) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/hash_to_range", payload = "${v}" } }]

[rules.size_format_date]
sql = '''SELECT format_date('millisecond', str(payload.tz), str(payload.fmt), payload.ts) AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/format_date", payload = "${v}" } }]

[rules.size_rfc3339]
sql = '''SELECT unix_ts_to_rfc3339(payload.ts, 'millisecond') AS v FROM "#"'''
actions = [{ function = "republish", args = { topic = "s/rfc3339", payload = "${v}" } }]

[rules.each_regex]
sql = '''FOREACH payload.a AS e DO e INCASE regex_match(str(e), str(payload.re)) FROM "#"'''
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
