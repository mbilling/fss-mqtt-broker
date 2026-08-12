---
adr: "0060"
title: "Bridge durability and acknowledgement contract"
adr_status: Proposed
tasks:
  - id: 0060-T1
    title: "Test: a full spool must not shed a message the bridge already accepted (and acked) to make room for a newer one"
    status: done
    date: 2026-08-12
    evidence: "crates/mqtt-bridge/tests/engine.rs a_full_spool_must_not_shed_a_message_the_bridge_already_accepted: a 1-slot spool, upstream down, publish FIRST then SECOND at QoS 1; on reconnect the upstream must replay FIRST. MUTATION-TESTED: forcing Overflow::DropOldest makes it replay SECOND instead (left=SECOND, right=FIRST) — the accepted message was silently shed."
  - id: 0060-T2
    title: "Ack moved off arrival: the engine acknowledges the source only after the durable outcome (spool accepted, or dispatched to a live destination); the spool refuses at the cap for QoS>=1 instead of shedding acked messages"
    status: done
    date: 2026-08-12
    evidence: "client.rs no longer PUBACKs in the read loop (the comment records why); new Command::Ack is issued by the engine router after the outcome, and a spool push failure withholds it (durably_accepted=false) so the source redelivers. spool.rs gains Overflow::{Refuse,DropOldest} + SpoolError::Full; build_spool picks Refuse when any QoS>=1 rule exists (ADR 0060 T5), DropOldest otherwise. Covered by T1 (mutation-tested) plus the existing spool-replay and QoS-1 forwarding tests."
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
    title: "Bridge throughput/latency benchmark + regression floor in bench/ (ADR 0048), captured before and after T8 — a hot-path performance claim needs evidence"
    status: planned
  - id: 0060-T8
    title: "Fast path waits for the downstream PUBACK: correlate the destination's pkid back to the source obligation, closing the dispatch->ack window (ADR 0060 §5.1-5.2)"
    status: planned
---

# Delivery — ADR 0060

> **Generated** progress table is produced by `scripts/gen-status.py`. This file holds the
> plan and its frontmatter; the dashboard renders from the task list above.

<!-- status-table:0060 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0060-T1 | ✅ done | 2026-08-12 | "crates/mqtt-bridge/tests/engine.rs a_full_spool_must_not_shed_a_message_the_bridge_already_accepted: a 1-slot spool, upstream down, publish FIRST then SECOND at QoS 1; on reconnect the upstream must replay FIRST. MUTATION-TESTED: forcing Overflow::DropOldest makes it replay SECOND instead (left=SECOND, right=FIRST) — the accepted message was silently shed." |
| 0060-T2 | ✅ done | 2026-08-12 | "client.rs no longer PUBACKs in the read loop (the comment records why); new Command::Ack is issued by the engine router after the outcome, and a spool push failure withholds it (durably_accepted=false) so the source redelivers. spool.rs gains Overflow::{Refuse,DropOldest} + SpoolError::Full; build_spool picks Refuse when any QoS>=1 rule exists (ADR 0060 T5), DropOldest otherwise. Covered by T1 (mutation-tested) plus the existing spool-replay and QoS-1 forwarding tests." |
| 0060-T3 | ✅ done | 2026-08-11 | "spool.rs push/drain set `wtx.set_durability(Durability::Immediate)` before commit (mirrors the broker's own durable stores, ADR 0018), so a QoS>=1 source ack can be gated on a real fsync." |
| 0060-T4 | ✅ done | 2026-08-11 | "config.rs requires_durable_spool() + validate(): a QoS>=1 rule with no [spool].dir and no allow_ephemeral_spool is rejected (test a_qos1_rule_requires_a_durable_spool). engine.rs build_spool refuses to start (error + exit) when the disk spool fails to open and durability is required, instead of the old silent in-memory fallback." |
| 0060-T5 | ✅ done | 2026-08-11 | "spool.rs push: when dropping the oldest at the cap, decode it and emit a bridge::audit event (topic, reason=spool-full) — a lost auditable-crossing message now leaves a trail, not just a counter." |
| 0060-T6 | ⬜ planned | — |  |
| 0060-T7 | ⬜ planned | — |  |
| 0060-T8 | ⬜ planned | — |  |
<!-- /status-table:0060 -->

Closes the durability/ack half of the bridge audit (epic #186, finding #5, issue #188). The
bridge analogue of [ADR 0057](../adr/0057-durable-outbound-inflight.md). Design in
[ADR 0060](../adr/0060-bridge-durability-and-ack-contract.md). Built test-first: T1 is the red
test that reproduces the ack-before-durable loss; T2 turns it green.

**Delivered: T1–T5.** The acknowledgement no longer fires on arrival — the engine sends it
only after the message is durably accepted (spooled, or dispatched to a live destination), and a
spool that cannot accept it withholds the ack so the source redelivers. At the cap a `QoS`≥1
spool now **refuses** the newcomer rather than shedding an already-acked message.

**Remaining: T8** (plus its docs T6 and evidence T7). On the fast path the ack still fires at
*dispatch*, not on the destination's PUBACK, so a crash in that window can lose a message the
source considered delivered. Closing it means correlating the destination's packet id back to
the source obligation — designed in
[ADR 0060 §5](../adr/0060-bridge-durability-and-ack-contract.md): the fast path is satisfied by
the *downstream ack* (no disk I/O added — the naive "fsync every message" reading would be a
serious regression), the read loop never blocks (a pending-ack table keeps pipelining), spooling
uses group commit, and the in-flight window is bounded by an advertised Receive Maximum. The
trade is latency per `QoS`≥1 message, not throughput.

**T7 exists because that is a performance claim about a hot path.** It lands with a bridge
throughput/latency benchmark and a regression floor in `bench/` (ADR 0048), measured before and
after T8 — this project's standard is evidence, not assertion.
