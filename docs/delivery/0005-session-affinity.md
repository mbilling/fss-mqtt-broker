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
    status: done
    date: 2026-07-02
    evidence: "Verification-close: the workstream this task was deferred to (ADR 0006/0007, durable-by-default per ADR 0029) delivered exactly what P2d asked for, so this closes on that evidence rather than new code. Owner death mid-session necessarily drops the live CONNECTION (the spliced TCP path terminates at the owner), but the SESSION — subscriptions, queued messages, the QoS-2 dedup window, and the session-expiry deadline — lives in the quorum-replicated store and is served by the new owner on reconnect. Proven by durable_sessions.rs a_persistent_client_resumes_its_session_on_the_new_owner_after_takeover, a_queued_message_is_replayed_to_the_client_after_takeover, qos2_inbound_dedup_survives_owner_takeover, a_replica_serves_the_session_after_the_owner_dies, plus 0001-T10 (expiry deadline survives takeover). Honest residual: a client reconnect RACING the promotion window and spec-legal QoS-1 redelivery bounds remain 0001-T11 (deferred there, not here)."
  - id: 0005-P3
    title: MQTT 5 Server-Reference redirect replacing the relay for v5 clients
    status: deferred
    notes: "Re-assessed 2026-07-02: the original blocker (no v5 codec) is gone (ADR 0008), so this is now buildable — but parked on the OTHER half of the original condition: mainstream v5 clients (paho, mosquitto) do not auto-follow Server Reference / 0x9C redirects, so the relay must remain the universal path regardless and a redirect would only serve clients that opt into handling it. Revisit if a redirect-capable client population materialises; the proxy serves 3.1.1 and v5 alike meanwhile."
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
| **0005-P2d** Durable relocation | A relocated session survives owner death: the connection drops (inherent — the splice ends at the owner) but the session state is recovered from the replicated store by the new owner on reconnect. Satisfied by the durable workstream (ADR 0006/0007/0029). |
| **0005-P3** Server-Reference | MQTT 5 Server-Reference lets v5 clients reconnect to the owner directly, retiring the relay for v5. |

## Progress

<!-- status-table:0005 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0005-P1 | ✅ done | 2026-06-12 | placement.rs Placement::owner / owns; swim_routing.rs membership-driven placement |
| 0005-P2 | ✅ done | 2026-06-12 | swim_routing::persistent_session_is_relocated_to_its_owner; conn.rs serve_proxied / PeerMessage::ProxyHello |
| 0005-P2b | ✅ done | 2026-06-12 | conn.rs serve_proxied -> run_framed -> authenticate_connect records auth.success "relayed by node {via}" |
| 0005-P2c | 💤 deferred | — | splice is best-effort on half-close; a delivery/lifecycle hardening pass is a documented follow-up |
| 0005-P2d | ✅ done | 2026-07-02 | "Verification-close: the workstream this task was deferred to (ADR 0006/0007, durable-by-default per ADR 0029) delivered exactly what P2d asked for, so this closes on that evidence rather than new code. Owner death mid-session necessarily drops the live CONNECTION (the spliced TCP path terminates at the owner), but the SESSION — subscriptions, queued messages, the QoS-2 dedup window, and the session-expiry deadline — lives in the quorum-replicated store and is served by the new owner on reconnect. Proven by durable_sessions.rs a_persistent_client_resumes_its_session_on_the_new_owner_after_takeover, a_queued_message_is_replayed_to_the_client_after_takeover, qos2_inbound_dedup_survives_owner_takeover, a_replica_serves_the_session_after_the_owner_dies, plus 0001-T10 (expiry deadline survives takeover). Honest residual: a client reconnect RACING the promotion window and spec-legal QoS-1 redelivery bounds remain 0001-T11 (deferred there, not here)." |
| 0005-P3 | 💤 deferred | — | "Re-assessed 2026-07-02: the original blocker (no v5 codec) is gone (ADR 0008), so this is now buildable — but parked on the OTHER half of the original condition: mainstream v5 clients (paho, mosquitto) do not auto-follow Server Reference / 0x9C redirects, so the relay must remain the universal path regardless and a redirect would only serve clients that opt into handling it. Revisit if a redirect-capable client population materialises; the proxy serves 3.1.1 and v5 alike meanwhile." |
<!-- /status-table:0005 -->

**Note carried from the ADR (resolved):** phase 2 shipped in *ephemeral mode* — sharded
session capacity without durability across owner loss. That gap was ADR 0006's workstream E,
which has since landed (durable-by-default, ADR 0029); P2d is closed on its evidence above.

## Changelog

- **2026-07-02** — P2d closed by **verification**, not new code: the durable workstream it
  was deferred to (ADR 0006/0007, durable-by-default per ADR 0029) delivers durability
  across owner loss for relocated sessions — the connection drops when the owner dies (the
  splice ends there), but the session's subscriptions, queued messages, QoS-2 window, and
  expiry deadline are recovered by the new owner on reconnect (takeover tests in
  `durable_sessions.rs`; residual reconnect-races-promotion hardening tracked as 0001-T11).
  P3 (v5 Server-Reference redirect) re-assessed: unblocked by the v5 codec but kept
  deferred — mainstream v5 clients do not auto-follow redirects, so the relay stays the
  universal path either way. P2c (splice hardening) remains the only open 0005 item.
- **2026-06-12** — Phase 2 proxy data plane landed (P2): relocation over the peer mesh,
  identity vouching via `ProxyHello`, CONNECT replay and stream splice, proven end to end.
  Audit `via=<node>` (P2b) landed with it. Splice hardening (P2c), durable relocation (P2d),
  and MQTT 5 Server-Reference (P3) split out as follow-ups.
- **2026-06-12** — Phase 1 placement + ownership awareness landed (P1).
