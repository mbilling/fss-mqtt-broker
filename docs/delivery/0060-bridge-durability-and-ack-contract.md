---
adr: "0060"
title: "Bridge durability and acknowledgement contract"
adr_status: Proposed
tasks:
  - id: 0060-T1
    title: "Red test: a crash/failure injected between the source PUBACK and the spool commit loses the acked message"
    status: planned
  - id: 0060-T2
    title: "Ack-on-durable: pending-ack model — source PUBACK emitted by a completion callback only after spool fsync-commit or a downstream QoS>=1 ack"
    status: planned
  - id: 0060-T3
    title: "Explicit fsync-on-commit spool durability, asserted in code and tested (not left to a redb default)"
    status: planned
  - id: 0060-T4
    title: "Remove silent in-memory fallback: a QoS>=1 rule with no durable spool refuses to start, or runs under `allow_ephemeral_spool` with loud logging"
    status: planned
  - id: 0060-T5
    title: "Audit record on spool drop (topic/direction/upstream/reason) into the ADR 0025 §8 stream; `overflow = drop-oldest | refuse` per-rule, default `refuse` for QoS>=1"
    status: planned
  - id: 0060-T6
    title: "Docs: ADR 0025 §7 Consequences amendment stating the durability contract and overflow behaviour"
    status: planned
---

# Delivery — ADR 0060

> **Generated** progress table is produced by `scripts/gen-status.py`. This file holds the
> plan and its frontmatter; the dashboard renders from the task list above.

<!-- status-table:0060 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0060-T1 | ⬜ planned | — |  |
| 0060-T2 | ⬜ planned | — |  |
| 0060-T3 | ⬜ planned | — |  |
| 0060-T4 | ⬜ planned | — |  |
| 0060-T5 | ⬜ planned | — |  |
| 0060-T6 | ⬜ planned | — |  |
<!-- /status-table:0060 -->

Closes the durability/ack half of the bridge audit (epic #186, finding #5, issue #188). The
bridge analogue of [ADR 0057](../adr/0057-durable-outbound-inflight.md). Design in
[ADR 0060](../adr/0060-bridge-durability-and-ack-contract.md). Built test-first: T1 is the red
test that reproduces the ack-before-durable loss; T2 turns it green.
