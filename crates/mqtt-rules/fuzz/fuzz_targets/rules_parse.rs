#![no_main]
//! Fuzz target: loading a rules file must never panic or hang.
//!
//! The rules file is operator input, re-read on every reload (ADR 0083): a malformed
//! one must be refused with an error, never take the broker down. Every statement
//! that does load is also evaluated once against a small message, so the evaluator
//! sees every shape the parser can produce.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    // As a whole file, and as one bare statement.
    if let Ok(loaded) = mqtt_rules::RuleSet::parse(text) {
        let payload = bytes::Bytes::from_static(br#"{"a":[1,2,{"b":"c"}],"x":1.5}"#);
        let props = mqtt_core::AppProperties::default();
        let input = mqtt_rules::PublishInput::new("c", "t/a", &payload, 1, &props);
        let mut out = Vec::new();
        loaded.rules.on_publish(&input, &mut |_, _| {}, &mut out);
    }
    let payload = bytes::Bytes::from_static(b"{}");
    let props = mqtt_core::AppProperties::default();
    let input = mqtt_rules::PublishInput::new("c", "t/a", &payload, 0, &props);
    let _ = mqtt_rules::test_sql(text, &input);
});
