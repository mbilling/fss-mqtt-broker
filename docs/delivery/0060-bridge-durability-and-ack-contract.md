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
    title: "Explicit fsync-on-commit spool durability (not left to a redb default)"
    status: done
    date: 2026-08-11
    evidence: "spool.rs push/drain set `wtx.set_durability(Durability::Immediate)` before commit (mirrors the broker's own durable stores, ADR 0018), so a QoS>=1 source ack can be gated on a real fsync."
  - id: 0060-T4
    title: "No silent in-memory fallback: a QoS>=1 rule with no durable spool is refused, unless `allow_ephemeral_spool`"
    status: done
    date: 2026-08-11
    evidence: "config.rs requires_durable_spool() + validate(): a QoS>=1 rule with no [spool].dir and no allow_ephemeral_spool is rejected (test a_qos1_rule_requires_a_durable_spool). engine.rs build_spool refuses to start (error + exit) when the disk spool fails to open and durability is required, instead of the old silent in-memory fallback."
  - id: 0060-T5
    title: "Audit record on a spool-full drop (topic + reason) into the ADR 0025 §8 stream"
    status: done
    date: 2026-08-11
    evidence: "spool.rs push: when dropping the oldest at the cap, decode it and emit a bridge::audit event (topic, reason=spool-full) — a lost auditable-crossing message now leaves a trail, not just a counter."
  - id: 0060-T1
    title: "Test: a crash between the source PUBACK and the spool commit loses the acked message"
    status: planned
  - id: 0060-T2
    title: "Ack-on-durable: pending-ack model — source PUBACK only after spool fsync-commit or a downstream ack (needs the store-and-forward queue redesign: spool-then-drain with per-message removal)"
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
| 0060-T3 | ✅ done | 2026-08-11 | "spool.rs push/drain set `wtx.set_durability(Durability::Immediate)` before commit (mirrors the broker's own durable stores, ADR 0018), so a QoS>=1 source ack can be gated on a real fsync." |
| 0060-T4 | ✅ done | 2026-08-11 | "config.rs requires_durable_spool() + validate(): a QoS>=1 rule with no [spool].dir and no allow_ephemeral_spool is rejected (test a_qos1_rule_requires_a_durable_spool). engine.rs build_spool refuses to start (error + exit) when the disk spool fails to open and durability is required, instead of the old silent in-memory fallback." |
| 0060-T5 | ✅ done | 2026-08-11 | "spool.rs push: when dropping the oldest at the cap, decode it and emit a bridge::audit event (topic, reason=spool-full) — a lost auditable-crossing message now leaves a trail, not just a counter." |
| 0060-T1 | ⬜ planned | — |  |
| 0060-T2 | ⬜ planned | — |  |
| 0060-T6 | ⬜ planned | — |  |
<!-- /status-table:0060 -->

Closes the durability/ack half of the bridge audit (epic #186, finding #5, issue #188). The
bridge analogue of [ADR 0057](../adr/0057-durable-outbound-inflight.md). Design in
[ADR 0060](../adr/0060-bridge-durability-and-ack-contract.md). Built test-first: T1 is the red
test that reproduces the ack-before-durable loss; T2 turns it green.
