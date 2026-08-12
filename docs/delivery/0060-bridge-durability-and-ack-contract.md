---
adr: "0060"
title: "Bridge durability and acknowledgement contract"
adr_status: Proposed
tasks:
  - id: 0060-T1
    title: "Red test: a failure injected between the source PUBACK and the spool commit loses the acked message"
    status: planned
  - id: 0060-T2
    title: "Ack-on-durable: pending-ack table (source pkid -> obligations), released on a downstream ack (fast path, no disk) or a spool group-commit; read loop never blocks; Receive Maximum bounds the window (ADR 0060 §5)"
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
  - id: 0060-T6
    title: "Docs: ADR 0025 §7 Consequences amendment stating the durability contract and overflow behaviour"
    status: planned
  - id: 0060-T7
    title: "Bridge throughput/latency benchmark + regression floor in bench/ (ADR 0048), captured before and after T2 — a hot-path performance claim needs evidence"
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
| 0060-T6 | ⬜ planned | — |  |
| 0060-T7 | ⬜ planned | — |  |
<!-- /status-table:0060 -->

Closes the durability/ack half of the bridge audit (epic #186, finding #5, issue #188). The
bridge analogue of [ADR 0057](../adr/0057-durable-outbound-inflight.md). Design in
[ADR 0060](../adr/0060-bridge-durability-and-ack-contract.md). Built test-first: T1 is the red
test that reproduces the ack-before-durable loss; T2 turns it green.

**T3/T4/T5 are delivered.** What remains is the ack-timing change itself (T1/T2), plus its
docs (T6) and its evidence (T7).

**T2 is a hot-path change, so the design is specified up front** in
[ADR 0060 §5](../adr/0060-bridge-durability-and-ack-contract.md): the fast path is satisfied by
the *downstream ack* (no disk I/O added — the naive "fsync every message" reading would be a
serious regression), the read loop never blocks (a pending-ack table keeps pipelining), spooling
uses group commit, and the in-flight window is bounded by an advertised Receive Maximum. The
trade is latency per `QoS`≥1 message, not throughput.

**T7 exists because that is a performance claim about a hot path.** It lands with a bridge
throughput/latency benchmark and a regression floor in `bench/` (ADR 0048), measured before and
after T2 — this project's standard is evidence, not assertion.
