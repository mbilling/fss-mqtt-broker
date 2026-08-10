---
adr: "0057"
title: "Durable outbound in-flight state: exactly-once across a broker crash"
adr_status: Proposed
tasks:
  - id: 0057-T1
    title: "`SessionStore` outbound in-flight table: record / advance / clear / read, replicated with the session"
    status: planned
  - id: 0057-T2
    title: "Hub wiring: write at allocation and PUBREC, clear at PUBCOMP, fail closed with the ack withheld when the write fails"
    status: planned
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
| 0057-T1 | ⬜ planned | — |  |
| 0057-T2 | ⬜ planned | — |  |
| 0057-T3 | ⬜ planned | — |  |
| 0057-T4 | ⬜ planned | — |  |
| 0057-T5 | ⬜ planned | — |  |
| 0057-T6 | ⬜ planned | — |  |
<!-- /status-table:0057 -->
