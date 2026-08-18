---
adr: "0063"
title: "External integration without a rule engine: the consumer-group pattern"
adr_status: Accepted
tasks:
  - id: 0063-T1
    title: The blueprint — docs/INTEGRATION.md, the external-consumer reference architecture
    status: done
    date: 2026-08-18
    evidence: "docs/INTEGRATION.md: the pattern (a `$share` consumer group at QoS 1 with durable persistent sessions), its five-setting contract with why each is load-bearing, what the broker guarantees AND refuses (queue-while-down with the ADR 0041 cap and drop-oldest ack-and-drop named as the loss signal, `$share` per-message interleaving with the partitioned-ownership alternative reusing the ADR 0059 lesson, retained skipping shared subscriptions per MQTT-3.8.4 with the plain-subscribe bootstrap), the exactly-once-ish recipe (publisher-assigned id, ack-after-sink-accept mirroring ADR 0060, idempotent write), the Kafka and webhook lanes (stock-connector checklist + a forwarder sketch explicitly labelled a sketch, DLQ back into MQTT at QoS 1), composition with the boundary bridge across a trust zone, the rule-engine construct mapping table for migrators, least-privilege + the four PromQL watch expressions, and a what-you-do-NOT-get section that ends by recommending a rule-engine broker when operating forwarders is the dominant cost. Every metric name, env var, selection rule and QoS behaviour cited was verified against the tree before being written (ADR 0010 §2 selection, mqtt-observability metric registry, ADR 0041 defaults)."
  - id: 0063-T2
    title: The blueprint executed against the real binary — integration-consumer-smoke + CI + mqttui declaration
    status: done
    date: 2026-08-18
    evidence: "scripts/integration-consumer-smoke.sh (quickstart-smoke conventions: ephemeral ports, polled readiness, stock mosquitto clients, MQTTD_BIN reuse): (1) two members of one `$share` group — one MQTT 5, one 3.1.1, pinning the doc's 3.1.1-members claim — split a 10-message QoS 1 stream delivered exactly once across the group (observed 5/5 round-robin; asserted: total exactly N, every payload present, both members ≥1); (2) a retained value is NOT replayed to a new shared subscription and IS delivered to the documented plain-subscribe bootstrap; (3) with every member disconnected (and the 3.1.1 member's session deliberately wiped by a clean-start takeover so the fallback target is deterministic), 5 QoS 1 publishes are accepted, queued to the surviving persistent session, and replayed IN ORDER on reconnect. Wired into ci.yml beside the quickstart smoke and declared in tools/mqttui/tasks.toml (id integration-consumer). Building it caught a real harness bug worth recording: backgrounding a shell WRAPPER FUNCTION makes `$!` the wrapper subshell, so killing it orphans the still-connected client, which then fights its successor in exactly the takeover loop BRIDGE.md documents — the script now backgrounds mosquitto_sub directly and says why."
  - id: 0063-T3
    title: Spec alignment — COMPARISON, README and MIGRATION point at the pattern instead of a blank
    status: done
    date: 2026-08-18
    evidence: "docs/COMPARISON.md rule-engine row: '✖ not planned — boundary bridge + standard integrations instead' → '✖ by design — a documented, CI-tested external-consumer pattern instead (INTEGRATION.md)'; the 'Do not choose mqttd (yet)' paragraph now hands the rule-engine migrator the blueprint link. README: the capability-map 'Where we lose' row links the blueprint, and the counts guarded by check-readme-facts.py were bumped with the tree (ADRs 62→63, runnable scripts 38→39). docs/MIGRATION.md 'What deliberately does not' (EMQX): the every-sink-becomes-a-consumer-you-own bullet now links the blueprint that designs that consumer. check-readme-facts.py green."
---

# Delivery 0063 — External integration without a rule engine

**ADR:** [docs/adr/0063-external-consumer-integration.md](../adr/0063-external-consumer-integration.md)

The rule-engine replacement story, delivered as issue #251's preferred exit: not a
Kafka sink in the broker, but a committed, executable blueprint — the
external-consumer pattern in [docs/INTEGRATION.md](../INTEGRATION.md), proven in CI by
`scripts/integration-consumer-smoke.sh`, with COMPARISON/README/MIGRATION aligned to
point at it. First-class bridge sinks remain the recorded revisit trigger (ADR 0063).

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0063-T1** Blueprint | A reference-architecture document for the external-consumer pattern: which topics, which QoS, session shape, how the durable queue buffers a down consumer, how to get exactly-once-ish semantics into Kafka/webhooks, ordering options, trust-boundary composition with the bridge, and the honest costs — every stated behaviour verified against the tree. |
| **0063-T2** Executable proof | The blueprint's load-bearing claims run against the real binary in CI (group single delivery incl. a 3.1.1 member, retained-skips-`$share`, in-order queue-while-down replay), declared in the mqttui manifest. |
| **0063-T3** Spec alignment | COMPARISON's rule-engine row, the README capability map, and MIGRATION's EMQX sink guidance reference the pattern; `check-readme-facts.py` stays green. |

## Progress

<!-- status-table:0063 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0063-T1 | ✅ done | 2026-08-18 | "docs/INTEGRATION.md: the pattern (a `$share` consumer group at QoS 1 with durable persistent sessions), its five-setting contract with why each is load-bearing, what the broker guarantees AND refuses (queue-while-down with the ADR 0041 cap and drop-oldest ack-and-drop named as the loss signal, `$share` per-message interleaving with the partitioned-ownership alternative reusing the ADR 0059 lesson, retained skipping shared subscriptions per MQTT-3.8.4 with the plain-subscribe bootstrap), the exactly-once-ish recipe (publisher-assigned id, ack-after-sink-accept mirroring ADR 0060, idempotent write), the Kafka and webhook lanes (stock-connector checklist + a forwarder sketch explicitly labelled a sketch, DLQ back into MQTT at QoS 1), composition with the boundary bridge across a trust zone, the rule-engine construct mapping table for migrators, least-privilege + the four PromQL watch expressions, and a what-you-do-NOT-get section that ends by recommending a rule-engine broker when operating forwarders is the dominant cost. Every metric name, env var, selection rule and QoS behaviour cited was verified against the tree before being written (ADR 0010 §2 selection, mqtt-observability metric registry, ADR 0041 defaults)." |
| 0063-T2 | ✅ done | 2026-08-18 | "scripts/integration-consumer-smoke.sh (quickstart-smoke conventions: ephemeral ports, polled readiness, stock mosquitto clients, MQTTD_BIN reuse): (1) two members of one `$share` group — one MQTT 5, one 3.1.1, pinning the doc's 3.1.1-members claim — split a 10-message QoS 1 stream delivered exactly once across the group (observed 5/5 round-robin; asserted: total exactly N, every payload present, both members ≥1); (2) a retained value is NOT replayed to a new shared subscription and IS delivered to the documented plain-subscribe bootstrap; (3) with every member disconnected (and the 3.1.1 member's session deliberately wiped by a clean-start takeover so the fallback target is deterministic), 5 QoS 1 publishes are accepted, queued to the surviving persistent session, and replayed IN ORDER on reconnect. Wired into ci.yml beside the quickstart smoke and declared in tools/mqttui/tasks.toml (id integration-consumer). Building it caught a real harness bug worth recording: backgrounding a shell WRAPPER FUNCTION makes `$!` the wrapper subshell, so killing it orphans the still-connected client, which then fights its successor in exactly the takeover loop BRIDGE.md documents — the script now backgrounds mosquitto_sub directly and says why." |
| 0063-T3 | ✅ done | 2026-08-18 | "docs/COMPARISON.md rule-engine row: '✖ not planned — boundary bridge + standard integrations instead' → '✖ by design — a documented, CI-tested external-consumer pattern instead (INTEGRATION.md)'; the 'Do not choose mqttd (yet)' paragraph now hands the rule-engine migrator the blueprint link. README: the capability-map 'Where we lose' row links the blueprint, and the counts guarded by check-readme-facts.py were bumped with the tree (ADRs 62→63, runnable scripts 38→39). docs/MIGRATION.md 'What deliberately does not' (EMQX): the every-sink-becomes-a-consumer-you-own bullet now links the blueprint that designs that consumer. check-readme-facts.py green." |
<!-- /status-table:0063 -->

## Changelog

- **2026-08-18** — ADR accepted; T1–T3 landed together (the blueprint, its CI smoke,
  and the spec alignment are one change: a documented recipe without its proof, or a
  claim without its document, would each be the defect issue #251 names).
