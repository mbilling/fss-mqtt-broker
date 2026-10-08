---
adr: "0084"
title: "Watching and editing rules live"
adr_status: Accepted
tasks:
  - id: 0084-T1
    title: "$SYS/ reserved for the broker: client publishes, Wills and rule republishes refused, the hub routes it only from SysPublish, retained leftovers purged, the authz dry run agrees"
    status: done
    date: 2026-10-08
    evidence: "EVIDENCE-PENDING: mqtt_core::is_reserved_topic test; conn tests (v5 0x87 incl. through a topic alias, QoS 2 PUBREC 0x87, v3.1.1 acked and dropped, Will CONNACK 0x87); mqtt-rules republish refusal and the two load warnings; one hub test per non-SysPublish path; the boot purge count; the authz explain test; the dropped-message counter name"
  - id: 0084-T2
    title: "An ACL subscribe deny also matches the filter inside $share/<g>/<f>"
    status: done
    date: 2026-10-08
    evidence: "EVIDENCE-PENDING: mqtt-auth acl tests (deny $SYS/# refuses $share/g/$SYS/#, deny a/# refuses $share/g/a/b, an explicit $share allow still admits) and the explain-agreement test over shared targets"
  - id: 0084-T3
    title: "A rules file's regex compile budget: identical literals compiled once, at most 96 distinct literals per file"
    status: done
    date: 2026-10-08
    evidence: "EVIDENCE-PENDING: mqtt-rules tests (a file one past the budget fails fast at the literal's position; many copies of one worst-case pattern load) and the measurement behind 96"
  - id: 0084-T4
    title: "Opt-in per-rule statistics on $SYS/brokers/<node>/rules[/<id>], live from the committed config, charged to node-pool credit"
    status: done
    date: 2026-10-08
    evidence: "EVIDENCE-PENDING: mqtt-config tests (overlay, ranges, node-id check, writers parsing, ENV_VARS count); rules_sys in-process tests (summary and per-rule messages with counts, rates and last_active_at, disabled rules listed, no series created, an interval change applied at once, a skipped tick counted, last reload repeats, last_error kinds and the synthesized delivery entry, a secret-bearing config error never on $SYS); the binary test with a real subscriber"
  - id: 0084-T5
    title: "An opt-in rule trace on $SYS/brokers/<node>/trace/rules/<id>, rate-limited per rule and per node, bounded in records and bytes"
    status: done
    date: 2026-10-08
    evidence: "EVIDENCE-PENDING: Rule::trace_due tests; trace tests (off sends nothing; publish, event and Will records with truncation and base64; per-rule rate, node ceiling and byte budget; $SYS publishes never run rules)"
  - id: 0084-T6
    title: "Admin API rules reads, check and test: a viewer-redacted listing, operator source, check and test with no side effects, and rules digests on /statusz and the cluster view"
    status: done
    date: 2026-10-08
    evidence: "EVIDENCE-PENDING: admin API tests (GET rules viewer vs operator, source, check valid and invalid with scope and positions, the per-rule form, test against the running set, a source and one rule forced on, topic-reserved 400, no_match reason, a dry run leaving no series, last_error, trace or WARN slot; body limits by role); /statusz rules block; cluster same_rules"
  - id: 0084-T7
    title: "Admin API rules writes for an operator listed in admin_writers: an atomic file replace and the ordinary reload, audited; the rules, rules-source, rules-apply and rule-delete verbs"
    status: done
    date: 2026-10-08
    evidence: "EVIDENCE-PENDING: admin API write tests (PUT file applied with digests, 412, 422, 428, 403 viewer and not-a-writer, 409 unwritable on a read-only directory; PUT and DELETE rule keep the rest byte for byte; mode kept; .prev written; rules.write audited); mqtt_rules::edit tests; CLI tests; the binary test where an admin edit applies live"
  - id: 0084-T8
    title: "A live simulator: the rules demo's ten minutes replayed endlessly, in windows aligned to the wall clock"
    status: done
    date: 2026-10-08
    evidence: "EVIDENCE-PENDING: rules_demo tests (plan() offsets; live.py --dry-run --windows 2 --clock fixture is the fixture twice; per-client timestamps monotonic across a window in now mode); simulate.py --dry-run unchanged"
  - id: 0084-T9
    title: "The live demo stack: the broker, three simulators and a loopback-only rules editor that is an admin API client"
    status: done
    date: 2026-10-08
    evidence: "EVIDENCE-PENDING: demo/rules-live compose stack brought up end to end (statistics, trace, the UI's API proxy, an edit applied live); the UI's no-innerHTML test; the compose MQTTD_* key test"
  - id: 0084-T10
    title: "Documentation: ADR 0084, and every 'no $SYS' and 'never writes configuration' statement corrected"
    status: done
    date: 2026-10-08
    evidence: "ADR 0084, with Revisited-by notes in ADR 0081 and 0051; docs/RULES.md (Watch and edit rules live, the [rules] gotcha, the limits, $SYS republish and FROM notes, the Differences and Operating tables); ADMIN-API.md (rules endpoints, errors, limits, audit) and ADMIN-CLI.md (the four verbs); AUDIT-SCHEMA.md (rules.write, the $SYS cases of acl.deny.*); OPERATIONS.md; THREAT-MODEL.md (client-surface information disclosure, spoofing and DoS rows, control-plane rows and accepted risks); HARDENING.md; README, GUIDE, COMPARISON (re-dated), MIGRATION, TEST-PLAN, ARCHITECTURE, CLIENT-GUIDE, KUBERNETES, TROUBLESHOOTING and the docs index; check-reason-codes WHEN_EMITTED[0x87]; the EMQX and Mosquitto converters (Python and mqttui's Rust port) reworded for $SYS, their tests passing"
---

# Delivery 0084 — watching and editing rules live

**ADR:** [docs/adr/0084-watching-and-editing-rules-live.md](../adr/0084-watching-and-editing-rules-live.md)

The plan, progress, and changelog for ADR 0084. Task status lives in the frontmatter
above; the table below is generated from it.

## Plan

| Task | Acceptance |
|---|---|
| **0084-T1** `$SYS/` reserved (D1) | No client publish, Will or rule republish reaches a `$SYS` topic, whatever the ACL says; the hub routes one only from the broker's own command; the authz dry run never says allowed for one. |
| **0084-T2** `$share` and ACL denies (D2) | A deny on a filter also refuses the same filter shared; no allow is loosened. |
| **0084-T3** Regex budget (D3) | No rules file's regular expressions cost more than about a second and 256 MiB to compile; an identical pattern repeated costs nothing extra. |
| **0084-T4** Statistics (D4) | A subscriber sees each rule's counts and rates per node; nothing secret or payload-derived is on `$SYS` with the trace off; turning it on or off, or changing the interval, needs no restart; the publishes yield to clients under pressure. |
| **0084-T5** Trace (D5) | A subscriber granted a rule's trace topic sees what the rule made of a message; off costs one atomic load; on, it is bounded in rate, records and bytes, and yields to clients. |
| **0084-T6** Admin reads, check, test (D6) | An operator can read the rules file, check a candidate and dry-run any rule against a message without changing anything; a viewer sees counts but no SQL; every node's rules digest is visible from one. |
| **0084-T7** Admin writes and CLI (D6, D7) | A listed writer can replace the file or one rule, the change reaches the next publish, the file on disk is the record, and every write is audited; nobody else can write. |
| **0084-T8** Live simulator | The demo's devices publish without end, on the wall clock, and recover from a broker restart. |
| **0084-T9** Demo stack and UI (D8) | One command brings up a broker, simulators and an editor; a user's MQTT client sees the data, the statistics and the trace; an edit in the browser applies live; nothing is exposed beyond loopback by default. |
| **0084-T10** Documentation | No document left saying mqttd has no `$SYS` or never writes configuration; every new surface in the threat model and the hardening baseline. |

## Progress

<!-- status-table:0084 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0084-T1 | ✅ done | — | 2026-10-08 | "EVIDENCE-PENDING: mqtt_core::is_reserved_topic test; conn tests (v5 0x87 incl. through a topic alias, QoS 2 PUBREC 0x87, v3.1.1 acked and dropped, Will CONNACK 0x87); mqtt-rules republish refusal and the two load warnings; one hub test per non-SysPublish path; the boot purge count; the authz explain test; the dropped-message counter name" |
| 0084-T2 | ✅ done | — | 2026-10-08 | "EVIDENCE-PENDING: mqtt-auth acl tests (deny $SYS/# refuses $share/g/$SYS/#, deny a/# refuses $share/g/a/b, an explicit $share allow still admits) and the explain-agreement test over shared targets" |
| 0084-T3 | ✅ done | — | 2026-10-08 | "EVIDENCE-PENDING: mqtt-rules tests (a file one past the budget fails fast at the literal's position; many copies of one worst-case pattern load) and the measurement behind 96" |
| 0084-T4 | ✅ done | — | 2026-10-08 | "EVIDENCE-PENDING: mqtt-config tests (overlay, ranges, node-id check, writers parsing, ENV_VARS count); rules_sys in-process tests (summary and per-rule messages with counts, rates and last_active_at, disabled rules listed, no series created, an interval change applied at once, a skipped tick counted, last reload repeats, last_error kinds and the synthesized delivery entry, a secret-bearing config error never on $SYS); the binary test with a real subscriber" |
| 0084-T5 | ✅ done | — | 2026-10-08 | "EVIDENCE-PENDING: Rule::trace_due tests; trace tests (off sends nothing; publish, event and Will records with truncation and base64; per-rule rate, node ceiling and byte budget; $SYS publishes never run rules)" |
| 0084-T6 | ✅ done | — | 2026-10-08 | "EVIDENCE-PENDING: admin API tests (GET rules viewer vs operator, source, check valid and invalid with scope and positions, the per-rule form, test against the running set, a source and one rule forced on, topic-reserved 400, no_match reason, a dry run leaving no series, last_error, trace or WARN slot; body limits by role); /statusz rules block; cluster same_rules" |
| 0084-T7 | ✅ done | — | 2026-10-08 | "EVIDENCE-PENDING: admin API write tests (PUT file applied with digests, 412, 422, 428, 403 viewer and not-a-writer, 409 unwritable on a read-only directory; PUT and DELETE rule keep the rest byte for byte; mode kept; .prev written; rules.write audited); mqtt_rules::edit tests; CLI tests; the binary test where an admin edit applies live" |
| 0084-T8 | ✅ done | — | 2026-10-08 | "EVIDENCE-PENDING: rules_demo tests (plan() offsets; live.py --dry-run --windows 2 --clock fixture is the fixture twice; per-client timestamps monotonic across a window in now mode); simulate.py --dry-run unchanged" |
| 0084-T9 | ✅ done | — | 2026-10-08 | "EVIDENCE-PENDING: demo/rules-live compose stack brought up end to end (statistics, trace, the UI's API proxy, an edit applied live); the UI's no-innerHTML test; the compose MQTTD_* key test" |
| 0084-T10 | ✅ done | — | 2026-10-08 | "ADR 0084, with Revisited-by notes in ADR 0081 and 0051; docs/RULES.md (Watch and edit rules live, the [rules] gotcha, the limits, $SYS republish and FROM notes, the Differences and Operating tables); ADMIN-API.md (rules endpoints, errors, limits, audit) and ADMIN-CLI.md (the four verbs); AUDIT-SCHEMA.md (rules.write, the $SYS cases of acl.deny.*); OPERATIONS.md; THREAT-MODEL.md (client-surface information disclosure, spoofing and DoS rows, control-plane rows and accepted risks); HARDENING.md; README, GUIDE, COMPARISON (re-dated), MIGRATION, TEST-PLAN, ARCHITECTURE, CLIENT-GUIDE, KUBERNETES, TROUBLESHOOTING and the docs index; check-reason-codes WHEN_EMITTED[0x87]; the EMQX and Mosquitto converters (Python and mqttui's Rust port) reworded for $SYS, their tests passing" |
<!-- /status-table:0084 -->

## Changelog

- **2026-10-08** — ADR 0084 accepted and delivered in one change: the `$SYS/` reservation,
  the `$share` deny fix and the regex budget; opt-in rule statistics and trace on `$SYS`;
  the admin API's rules endpoints and CLI verbs; the live simulator, the demo stack and its
  editor; and the documentation.
