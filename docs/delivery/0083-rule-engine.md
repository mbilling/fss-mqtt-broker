---
adr: "0083"
title: "A rule engine on the publish path: EMQX rule SQL, evaluated once per message where it lands"
adr_status: Accepted
tasks:
  - id: 0083-T1
    title: "mqtt-rules: EMQX's rule SQL — grammar, evaluation semantics, built-in functions, templates and the republish/console actions"
    status: done
    date: 2026-10-07
    evidence: "crates/mqtt-rules: lexer/parser on rulesql's grammar and precedence (SELECT/FOREACH/DO/INCASE/FROM/WHERE, CASE both forms, paths with quoted keys, 1-based/negative indices, ranges, list literals, IN/NOT IN, =~, -- comments); evaluator on emqx_rule_runtime's semantics (selected-then-input lookup with undefined fall-through, payload JSON decoded once per message and shared by every rule, undefined vs null, number/string and atom/string comparison coercions, Erlang arithmetic and term order, EMQX's implicit aliases); 120 built-in functions (107 of the 124 in EMQX's reference plus its 13 legacy accessors), unknown function or arity refused at load; ${…} templates over the rule output; republish with EMQX's arg names and defaults. 48 unit tests whose statements, inputs and expected outputs are EMQX's documented examples (rule-sql-syntax, builtin-functions, events-and-fields), including hash_to_range and the date-format examples; every function name asserted present in docs/RULES.md. clippy pedantic clean."
  - id: 0083-T2
    title: "The publish path: evaluate on the connection task once per message at its landing node; derived publishes gated with the original at QoS 1/2; Wills and client/session events; no re-trigger"
    status: done
    date: 2026-10-07
    evidence: "conn.rs handle_publish evaluates after the ACL and before the hub; rules::send_derived gives each QoS>=1 derived message its own ack gate and rules::join_outcomes answers the publisher (all accepted -> ack; all refused -> the original's refusal; mixed/withheld -> withhold); the ingress permit rides the batch's last command; derived publishes carry no publisher and v5=false; the hub evaluates Wills (HubCommand::AttachRules) and posts derived publishes back through dispatch; client/connected, client/disconnected (normal/keepalive_timeout/tcp_closed/server_closed/shutdown), session/subscribed and session/unsubscribed raised from conn.rs. tests/rules.rs, 9 end-to-end tests over real sockets: transform beside the original + per-rule metrics; QoS 1 ack and QoS 1 delivery; QoS 2 rules fire once across a DUP resend; a brownout-refused derived message WITHHOLDS the original's ack (mutation-checked: ungating derived messages makes it fail); a rule republishing into its own FROM produces exactly one message; FOREACH fan-out; connect/subscribe/disconnect events in order; a Will runs rules; two-node cluster: 5 publishes -> exactly 5 originals and 5 derived copies on the far node and zero evaluations there. rules::tests covers the join table. cargo test -p mqtt-rules -p mqtt-config -p mqtt-observability -p mqttd: 61 binaries, 1020 passed, 0 failed."
  - id: 0083-T3
    title: "Operating it: [rules] file / MQTTD_RULES_FILE, validate-before-swap reload, --check-rules, --rule-test, --check-config --preflight, per-rule metrics and the rules checksum"
    status: done
    date: 2026-10-07
    evidence: "mqtt-config [rules] file + MQTTD_RULES_FILE (ENV_VARS 112 -> 113, CONFIGURATION.md regenerated); a rules file that does not load refuses the boot; Reloader::attach_rules folds the rules into the atomic reload (reload::tests::a_reload_swaps_the_rules_and_a_bad_rules_file_keeps_the_running_ones: swap reaches the watch, mqttd_rules_info moves, a bad file rejects the whole reload with `rules: …` and keeps the running set); host_checks loads it under --check-config --preflight; the MQTTD_CONFIG_WATCH watcher stats the rules file (main::tests::the_rules_file_is_file_watched); mqttd --check-rules [file] and mqttd --rule-test --sql … (KNOWN_FLAGS/validate_cli/usage); mqttd_rule_evaluations_total{rule,result}, mqttd_rule_actions_total{rule,result}, mqttd_rules_loaded, mqttd_rules_info{checksum}; a failing rule WARNs once per 10 s."
  - id: 0083-T4
    title: "Documentation: docs/RULES.md, and every 'no rule engine' statement corrected"
    status: done
    date: 2026-10-07
    evidence: "docs/RULES.md: where rules run and why it scales, the QoS 0/1/2 table and the withhold rule, the rules file, the SQL and field references, the function table, republish/console with EMQX's defaults (the ${qos}-reads-the-output trap called out), operations, measured per-publish cost, a 15-row differences-from-EMQX table, and migration. ADR 0063 §1 marked superseded in part. README, COMPARISON, EVALUATION, INTEGRATION, MIGRATION (incl. the emqx-rules.conf fixture provenance row), GUIDE, THREAT-MODEL (two client-surface rows, one control-plane row, one accepted risk), OPERATIONS, ARCHITECTURE, SECURITY, docs/README and the example TOML updated; check-readme-facts.py green."
  - id: 0083-T5
    title: "The EMQX converter carries rules: from-emqx.py --out-rules"
    status: done
    date: 2026-10-07
    evidence: "convert_rules/render_rules: SQL verbatim, republish args (qos/retain typed, mqtt_properties carried), console; a sink action, an unsupported FROM ($bridges/, an event mqttd does not raise) or a function mqttd lacks becomes a TODO (the rule commented out with the reason, so the file still loads); rule_engine engine-level keys reported. New fixture fixtures/emqx-rules.conf is EMQX's documented rule-configs.md examples plus the unsupported constructs. test-from-emqx.sh: the translated file parses, passes `mqttd --check-rules` (6 rules, 5 enabled), the config naming it passes --check-config, SQL and args carried verbatim, every gap a TODO, a translated statement runs under --rule-test. Fuzz pass extended to --out-rules and the new fixture (309 inputs, no hang); property sweep 57 cases green."
  - id: 0083-T6
    title: "Measure the per-publish cost of rule evaluation"
    status: done
    date: 2026-10-07
    evidence: "crates/mqtt-rules/benches/rules.rs (criterion, one core of the dev VM): rules loaded but none selects the topic 0.17 µs; WHERE rejects 1.4 µs; WHERE passes + one republish 1.8 µs; SELECT * + JSON republish 6.3 µs; FOREACH over 10 elements 15.8 µs. Paid on connection tasks, never on the hub loop; at the 75,000 msg/s single-node knee a matching rule on every publish is ~0.14 core across the runtime."
  - id: 0083-T7
    title: "Fuzz the rule engine's untrusted inputs: the rules file and evaluation over arbitrary payloads"
    status: done
    date: 2026-10-07
    evidence: "crates/mqtt-rules/fuzz: rules_parse (an arbitrary rules file, and every statement that loads evaluated once) and rules_eval (fixed rules reaching into the payload every way a rule can — paths, indices, ranges, FOREACH, payload-supplied regex patterns, conversions, templates — over an arbitrary payload and user property), with seeds; both added to the nightly fuzz matrix. Run locally on the pinned nightly-2026-07-22 for 4 minutes each (1.6M rules_eval and 2.0M rules_parse executions) with no finding."
---

# Delivery 0083 — the rule engine

**ADR:** [docs/adr/0083-rule-engine.md](../adr/0083-rule-engine.md)

The plan, progress, and changelog for ADR 0083. Task status lives in the frontmatter
above; the table below is generated from it.

## Plan

| Task | Acceptance |
|---|---|
| **0083-T1** The engine | EMQX's documented examples produce EMQX's documented outputs; an unknown function fails the load. |
| **0083-T2** The publish path | A rule's output reaches subscribers at every QoS and on every node; a QoS 1/2 publisher's ack waits for it; a rule cannot loop; a cluster evaluates each message once. |
| **0083-T3** Operating it | Rules hot-reload validate-before-swap; a rules file is checkable offline; per-rule metrics and a cross-node checksum exist. |
| **0083-T4** Documentation | One reference an EMQX user can port rules with, and no document left saying mqttd has no rule engine. |
| **0083-T5** The converter | An EMQX config's rules come out as a file the broker loads, with every gap a TODO. |
| **0083-T6** Cost | The per-publish cost of evaluation is measured, not asserted. |
| **0083-T7** Fuzzing | The rules file and payload evaluation have fuzz targets in the nightly tier. |

## Progress

<!-- status-table:0083 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0083-T1 | ✅ done | — | 2026-10-07 | "crates/mqtt-rules: lexer/parser on rulesql's grammar and precedence (SELECT/FOREACH/DO/INCASE/FROM/WHERE, CASE both forms, paths with quoted keys, 1-based/negative indices, ranges, list literals, IN/NOT IN, =~, -- comments); evaluator on emqx_rule_runtime's semantics (selected-then-input lookup with undefined fall-through, payload JSON decoded once per message and shared by every rule, undefined vs null, number/string and atom/string comparison coercions, Erlang arithmetic and term order, EMQX's implicit aliases); 120 built-in functions (107 of the 124 in EMQX's reference plus its 13 legacy accessors), unknown function or arity refused at load; ${…} templates over the rule output; republish with EMQX's arg names and defaults. 48 unit tests whose statements, inputs and expected outputs are EMQX's documented examples (rule-sql-syntax, builtin-functions, events-and-fields), including hash_to_range and the date-format examples; every function name asserted present in docs/RULES.md. clippy pedantic clean." |
| 0083-T2 | ✅ done | — | 2026-10-07 | "conn.rs handle_publish evaluates after the ACL and before the hub; rules::send_derived gives each QoS>=1 derived message its own ack gate and rules::join_outcomes answers the publisher (all accepted -> ack; all refused -> the original's refusal; mixed/withheld -> withhold); the ingress permit rides the batch's last command; derived publishes carry no publisher and v5=false; the hub evaluates Wills (HubCommand::AttachRules) and posts derived publishes back through dispatch; client/connected, client/disconnected (normal/keepalive_timeout/tcp_closed/server_closed/shutdown), session/subscribed and session/unsubscribed raised from conn.rs. tests/rules.rs, 9 end-to-end tests over real sockets: transform beside the original + per-rule metrics; QoS 1 ack and QoS 1 delivery; QoS 2 rules fire once across a DUP resend; a brownout-refused derived message WITHHOLDS the original's ack (mutation-checked: ungating derived messages makes it fail); a rule republishing into its own FROM produces exactly one message; FOREACH fan-out; connect/subscribe/disconnect events in order; a Will runs rules; two-node cluster: 5 publishes -> exactly 5 originals and 5 derived copies on the far node and zero evaluations there. rules::tests covers the join table. cargo test -p mqtt-rules -p mqtt-config -p mqtt-observability -p mqttd: 61 binaries, 1020 passed, 0 failed." |
| 0083-T3 | ✅ done | — | 2026-10-07 | "mqtt-config [rules] file + MQTTD_RULES_FILE (ENV_VARS 112 -> 113, CONFIGURATION.md regenerated); a rules file that does not load refuses the boot; Reloader::attach_rules folds the rules into the atomic reload (reload::tests::a_reload_swaps_the_rules_and_a_bad_rules_file_keeps_the_running_ones: swap reaches the watch, mqttd_rules_info moves, a bad file rejects the whole reload with `rules: …` and keeps the running set); host_checks loads it under --check-config --preflight; the MQTTD_CONFIG_WATCH watcher stats the rules file (main::tests::the_rules_file_is_file_watched); mqttd --check-rules [file] and mqttd --rule-test --sql … (KNOWN_FLAGS/validate_cli/usage); mqttd_rule_evaluations_total{rule,result}, mqttd_rule_actions_total{rule,result}, mqttd_rules_loaded, mqttd_rules_info{checksum}; a failing rule WARNs once per 10 s." |
| 0083-T4 | ✅ done | — | 2026-10-07 | "docs/RULES.md: where rules run and why it scales, the QoS 0/1/2 table and the withhold rule, the rules file, the SQL and field references, the function table, republish/console with EMQX's defaults (the ${qos}-reads-the-output trap called out), operations, measured per-publish cost, a 15-row differences-from-EMQX table, and migration. ADR 0063 §1 marked superseded in part. README, COMPARISON, EVALUATION, INTEGRATION, MIGRATION (incl. the emqx-rules.conf fixture provenance row), GUIDE, THREAT-MODEL (two client-surface rows, one control-plane row, one accepted risk), OPERATIONS, ARCHITECTURE, SECURITY, docs/README and the example TOML updated; check-readme-facts.py green." |
| 0083-T5 | ✅ done | — | 2026-10-07 | "convert_rules/render_rules: SQL verbatim, republish args (qos/retain typed, mqtt_properties carried), console; a sink action, an unsupported FROM ($bridges/, an event mqttd does not raise) or a function mqttd lacks becomes a TODO (the rule commented out with the reason, so the file still loads); rule_engine engine-level keys reported. New fixture fixtures/emqx-rules.conf is EMQX's documented rule-configs.md examples plus the unsupported constructs. test-from-emqx.sh: the translated file parses, passes `mqttd --check-rules` (6 rules, 5 enabled), the config naming it passes --check-config, SQL and args carried verbatim, every gap a TODO, a translated statement runs under --rule-test. Fuzz pass extended to --out-rules and the new fixture (309 inputs, no hang); property sweep 57 cases green." |
| 0083-T6 | ✅ done | — | 2026-10-07 | "crates/mqtt-rules/benches/rules.rs (criterion, one core of the dev VM): rules loaded but none selects the topic 0.17 µs; WHERE rejects 1.4 µs; WHERE passes + one republish 1.8 µs; SELECT * + JSON republish 6.3 µs; FOREACH over 10 elements 15.8 µs. Paid on connection tasks, never on the hub loop; at the 75,000 msg/s single-node knee a matching rule on every publish is ~0.14 core across the runtime." |
| 0083-T7 | ✅ done | — | 2026-10-07 | "crates/mqtt-rules/fuzz: rules_parse (an arbitrary rules file, and every statement that loads evaluated once) and rules_eval (fixed rules reaching into the payload every way a rule can — paths, indices, ranges, FOREACH, payload-supplied regex patterns, conversions, templates — over an arbitrary payload and user property), with seeds; both added to the nightly fuzz matrix. Run locally on the pinned nightly-2026-07-22 for 4 minutes each (1.6M rules_eval and 2.0M rules_parse executions) with no finding." |
<!-- /status-table:0083 -->

## Changelog

- **2026-10-07** — ADR 0083 accepted and delivered in one change: the `mqtt-rules` crate,
  publish-path integration, operations surface, documentation, the EMQX converter's
  `--out-rules`, the benchmark and the fuzz targets.
