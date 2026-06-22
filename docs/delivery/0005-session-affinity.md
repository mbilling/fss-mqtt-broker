---
adr: "0005"
title: Session affinity: relocate persistent sessions to their owner
adr_status: Accepted
tasks:
  - id: 0005-P1
    title: Live placement + ownership awareness; persistent CONNECT consults placement
    status: done
    date: 2026-06-12
    evidence: placement.rs Placement::owner / owns; swim_routing.rs membership-driven placement
  - id: 0005-P2
    title: Proxy data plane - relocate persistent session to owner over peer mesh (ProxyHello + vouch + CONNECT replay + splice)
    status: done
    date: 2026-06-12
    evidence: swim_routing::persistent_session_is_relocated_to_its_owner; conn.rs serve_proxied / PeerMessage::ProxyHello
  - id: 0005-P2b
    title: Audit via=<node> detail attributing the vouching node on the owner
    status: done
    date: 2026-06-12
    evidence: conn.rs serve_proxied -> run_framed -> authenticate_connect records auth.success "relayed by node {via}"
  - id: 0005-P2c
    title: Delivery/lifecycle hardening of the splice (best-effort on half-close)
    status: deferred
    notes: splice is best-effort on half-close; a delivery/lifecycle hardening pass is a documented follow-up
  - id: 0005-P2d
    title: Durability across owner loss (ephemeral mode until replication)
    status: deferred
    notes: owner death mid-session drops the session; durability is workstream E (ADR 0006), not this ADR
  - id: 0005-P3
    title: MQTT 5 Server-Reference redirect replacing the relay for v5 clients
    status: deferred
    notes: needs the v5 codec and v5 clients; the proxy serves 3.1.1 and v5 alike until then
---

# Delivery — ADR 0005: Session affinity: relocate persistent sessions to their owner

Decision: [docs/adr/0005-session-affinity.md](../adr/0005-session-affinity.md).

## Plan

The decision's "Implementation phasing" (three numbered phases) decomposes into these
tasks, with the ADR's explicitly carried limitations split out as their own deferred
items. Each carries a stable id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0005-P1** Placement awareness | A shared, membership-driven `Placement` ring in the broker; a `clean_session=0` CONNECT consults it (`owner` / `owns`); `clean_session=1` is served on the landing node and never consults placement. |
| **0005-P2** Proxy data plane | A persistent session whose owner is another node is relocated there over the peer mesh: the landing node opens a connection to the owner's peer listener, vouches for the authenticated identity with a `PeerMessage::ProxyHello`, replays the CONNECT, and splices the client stream to the owner, which runs the real session with re-proxy disabled. Proven end to end: a persistent client on a non-owner node is served by its owner and receives a publish across the relay. |
| **0005-P2b** Audit `via` | The owner records the vouching node id (`auth.success` gains `via=<node>`) alongside the vouched client identity. |
| **0005-P2c** Splice hardening | The splice survives half-close cleanly under a delivery/lifecycle hardening pass rather than being best-effort. |
| **0005-P2d** Durable relocation | A relocated session survives owner death (upgrades the ephemeral mode to durable). |
| **0005-P3** Server-Reference | MQTT 5 Server-Reference lets v5 clients reconnect to the owner directly, retiring the relay for v5. |

## Progress

<!-- status-table:0005 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0005-P1 | ✅ done | 2026-06-12 | placement.rs Placement::owner / owns; swim_routing.rs membership-driven placement |
| 0005-P2 | ✅ done | 2026-06-12 | swim_routing::persistent_session_is_relocated_to_its_owner; conn.rs serve_proxied / PeerMessage::ProxyHello |
| 0005-P2b | ✅ done | 2026-06-12 | conn.rs serve_proxied -> run_framed -> authenticate_connect records auth.success "relayed by node {via}" |
| 0005-P2c | 💤 deferred | — | splice is best-effort on half-close; a delivery/lifecycle hardening pass is a documented follow-up |
| 0005-P2d | 💤 deferred | — | owner death mid-session drops the session; durability is workstream E (ADR 0006), not this ADR |
| 0005-P3 | 💤 deferred | — | needs the v5 codec and v5 clients; the proxy serves 3.1.1 and v5 alike until then |
<!-- /status-table:0005 -->

**Note carried from the ADR:** phase 2 ships in *ephemeral mode* — sharded session
capacity without durability across owner loss. Durability (P2d) is ADR 0006's workstream E,
not this decision.

## Changelog

- **2026-06-12** — Phase 2 proxy data plane landed (P2): relocation over the peer mesh,
  identity vouching via `ProxyHello`, CONNECT replay and stream splice, proven end to end.
  Audit `via=<node>` (P2b) landed with it. Splice hardening (P2c), durable relocation (P2d),
  and MQTT 5 Server-Reference (P3) split out as follow-ups.
- **2026-06-12** — Phase 1 placement + ownership awareness landed (P1).
