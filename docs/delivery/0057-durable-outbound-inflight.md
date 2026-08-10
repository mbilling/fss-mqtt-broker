---
adr: "0057"
title: "Durable outbound in-flight state: exactly-once across a broker crash"
adr_status: Proposed
tasks:
  - id: 0057-T1
    title: "`SessionStore` outbound in-flight table: record / advance / clear / read, replicated with the session"
    status: done
    date: 2026-08-10
    evidence: "crates/mqtt-storage: record_outbound/advance_outbound/clear_outbound/outbound on SessionStore, with the write-before-send ordering contract in the trait docs (same as #124's) and the QoS-1 exclusion stated where someone would look for it. ReplicatedSessionStore keeps the window in the m/{client} metadata snapshot beside the inbound window — equally small and low-churn, bounded by receive-maximum — encoded as a backward-compatible tail after the ADR 0031 owner field. MemorySessionStore and both test FlakyStores implement it; the flaky ones gate the writes behind the same fault seam as enqueue, so T2's fail-closed test has its lever. 62 storage tests green."
    notes: "Two tests carry the load: a failover replica sharing only the log sees id, offset AND PHASE (asserted explicitly, because the phase is what decides PUBLISH+DUP versus PUBREL on resume — resuming in the wrong phase either re-sends a message the subscriber holds or releases one it never got); and a pre-0057 metadata record decodes with an empty outbound window rather than an error. The old record is constructed by BYTE-TRUNCATING a modern one at the owner field's end, not by trusting the encoder to remember what the old format was."
  - id: 0057-T2
    title: "Hub wiring: write at allocation and PUBREC, clear at PUBCOMP, fail closed with the ack withheld when the write fails"
    status: done
    date: 2026-08-10
    evidence: "crates/mqttd/src/hub.rs — send_qos_publish records the id durably BEFORE the PUBLISH reaches the wire (QoS 2 with a durable offset only); pub_rec advances the record durably BEFORE the PUBREL goes out; pub_comp clears it. Each failure has its own semantics and its own retry story: a failed record withholds the PUBLISH and requeues at the backlog FRONT (the message is already durable from #124, so nothing is lost — only deferred, in order); a failed advance withholds the PUBREL, and the subscriber's own PUBREC re-send is the retry; a failed clear is logged and tolerated, because the worst a leftover entry does is one spurious PUBREL on restore, which the subscriber answers with PUBCOMP (MQTT-4.3.3) — and THAT retries the clear. Four hub unit tests; 186 total green."
    notes: "Writing the deferral test exposed an ordering hole in the first design: pre-0057, backlog-non-empty implied quota-full (acks drain before the loop processes another publish), so fresh traffic could never overtake the backlog. A deferred delivery broke that invariant, and a new publish would have jumped ahead of it — per-client ordering is part of the contract. The deliver gate now also diverts on backlog-non-empty, and a publish that finds quota free retries the drain, making traffic the deferred delivery's retry clock instead of the next reconnect. The FlakyStore fault seam gained a SEPARATE fail_outbound lever, because the enqueue lever fires first and would mask the very path these tests exercise."
  - id: 0057-T3
    title: "Restore: rebuild `pending` from the table, resume at PUBLISH+DUP or PUBREL under the original id, seed the allocator past restored ids"
    status: planned
  - id: 0057-T4
    title: "The SIGKILL acceptance test, both phases (PUBREL under the known id after PUBREC; PUBLISH+DUP under the original id before it)"
    status: planned
  - id: 0057-T5
    title: "Measure the QoS 2 delta in the bench lane; record it; revisit the on-by-default decision if indefensible"
    status: planned
  - id: 0057-T6
    title: "Remove the Limitations entry the fix retires — the README claim and the code change together"
    status: planned
---

# Delivery — ADR 0057

Tracking issue: #130. The decision text is
[the ADR](../adr/0057-durable-outbound-inflight.md); this file records what is actually
built, task by task, with evidence.

<!-- status-table:0057 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0057-T1 | ✅ done | 2026-08-10 | "crates/mqtt-storage: record_outbound/advance_outbound/clear_outbound/outbound on SessionStore, with the write-before-send ordering contract in the trait docs (same as #124's) and the QoS-1 exclusion stated where someone would look for it. ReplicatedSessionStore keeps the window in the m/{client} metadata snapshot beside the inbound window — equally small and low-churn, bounded by receive-maximum — encoded as a backward-compatible tail after the ADR 0031 owner field. MemorySessionStore and both test FlakyStores implement it; the flaky ones gate the writes behind the same fault seam as enqueue, so T2's fail-closed test has its lever. 62 storage tests green." |
| 0057-T2 | ✅ done | 2026-08-10 | "crates/mqttd/src/hub.rs — send_qos_publish records the id durably BEFORE the PUBLISH reaches the wire (QoS 2 with a durable offset only); pub_rec advances the record durably BEFORE the PUBREL goes out; pub_comp clears it. Each failure has its own semantics and its own retry story: a failed record withholds the PUBLISH and requeues at the backlog FRONT (the message is already durable from #124, so nothing is lost — only deferred, in order); a failed advance withholds the PUBREL, and the subscriber's own PUBREC re-send is the retry; a failed clear is logged and tolerated, because the worst a leftover entry does is one spurious PUBREL on restore, which the subscriber answers with PUBCOMP (MQTT-4.3.3) — and THAT retries the clear. Four hub unit tests; 186 total green." |
| 0057-T3 | ⬜ planned | — |  |
| 0057-T4 | ⬜ planned | — |  |
| 0057-T5 | ⬜ planned | — |  |
| 0057-T6 | ⬜ planned | — |  |
<!-- /status-table:0057 -->
