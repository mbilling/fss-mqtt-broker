---
adr: "0040"
title: Revocation reaches live state (eviction on reload)
adr_status: Proposed
tasks:
  - id: 0040-T1
    title: Admission metadata + eviction primitive — connections record principal source and client-cert leaf serial; hub can terminate a session by client id (v5 DISCONNECT 0x87, v3.1.1 close)
    status: done
    date: 2026-07-05
    evidence: "New hub types Admission { subject, method, cert_serial, protocol } and AuthMethod (Anonymous/Password/Token/Certificate/Enhanced): the server-side revocable facts a reload sweep re-checks — the broker retains facts about the admission, never replayable credentials. HubCommand::Attach and PendingAttach carry the Admission (subsuming the ADR 0031 owner subject); the Online entry keeps it. Capture: mqtt_auth::mtls::serial_from_cert extracts the leaf serial; conn::CertAdmission (identity + serial) replaces the bare identity through tls_admission / cert_admission, threaded through every listener (TCP-TLS, WSS, QUIC — one shared extraction) and handle_stream/run_framed; authenticate() returns the AuthMethod alongside the Identity (enhanced-auth exchanges record Enhanced); a proxied session's vouched identity carries no serial (the landing node holds the actual TLS session — documented on the field). Eviction: HubCommand::Evict { client, reason } -> Hub::evict queues DISCONNECT 0x87 (v5 only; v3.1.1 has no server DISCONNECT) then routes through detach(_, _, graceful=false), so the will publishes and session retention (ADR 0009), backlog spill, and stale-detach conn_id guards behave exactly as any ungraceful end; the queued DISCONNECT drains to the wire before the dropped outbound closes the writer. Evicting an offline client is a no-op. Mechanism only — nothing calls evict yet except the command. Tests: eviction_disconnects_the_target_and_leaves_others_undisturbed (v5 victim gets 0x87 then close, will reaches a bystander whose own flow continues, offline eviction no-op with the hub still serving), evicting_a_v311_client_closes_without_a_disconnect_packet. Workspace green (768 tests), clippy zero warnings.
  - id: 0040-T2
    title: Identity sweep — a successful reload disconnects live clients whose cert serial is CRL'd, whose password user was removed, or whose principal the new connect-ACL denies; untouched sessions keep flowing
    status: planned
  - id: 0040-T3
    title: Grant sweep — a tightened subscribe-ACL removes matching grants from live routing and durable subscription sets (online and offline sessions) and stops queued replay; no disconnect for permission-only changes
    status: planned
  - id: 0040-T4
    title: Peer-bus revocation — peer acceptor/connector become reloadable (the ADR 0032 deferred item); links record the remote leaf serial and a cluster-CRL reload tears down revoked links; mesh reacts as to link loss
    status: planned
  - id: 0040-T5
    title: Audit/metrics + closure — security.evict audit events and revocation_evictions_total{kind}; reload audit gains the sweep summary; admission-side durable-resume block pinned by test; README ops note
    status: planned
---

# Delivery — ADR 0040: Revocation reaches live state

Decision: [docs/adr/0040-revocation-reaches-live-state.md](../adr/0040-revocation-reaches-live-state.md).

Pre-release area ② (see ADR 0038's changelog for the four-area plan). Every revocation
mechanism today enforces at admission time; this delivery makes a successful policy
reload sweep **live** state — open client sessions, existing subscription grants,
established peer links — with the two-tier rule: identity revoked → session terminated,
permission tightened → grant removed.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0040-T1** Metadata + eviction | Connections retain principal source (anonymous/password/JWT/mTLS) and leaf serial when a client cert was presented; a hub command terminates a named session (v5 reason `0x87`; v3.1.1 close) reusing the takeover close path; no behavior change yet (mechanism only). |
| **0040-T2** Identity sweep | With a live connected client: publishing a CRL revoking its cert, deleting its password user, or denying its principal in the connect-ACL — each followed by a reload — disconnects it **without any client action**, while a second untouched client keeps flowing. Each eviction audited with its reason. |
| **0040-T3** Grant sweep | A subscriber receiving matching messages stops receiving them after a reload tightens its subscribe-ACL — subscription removed from routing **and** from the durable session set (proven for an offline session too: no replay of the revoked filter's queue on resume); the client is not disconnected; its next SUBSCRIBE is denied. |
| **0040-T4** Peer-bus revocation | Peer acceptor/connector rebuilt on reload (rotated cluster cert served on next peer handshake); a cluster-CRL reload tears down an established link to the revoked node in a two-node cluster; the revoked node cannot re-handshake; the healthy node's clients are undisturbed. |
| **0040-T5** Audit/metrics + closure | `security.evict` events carry kind + trigger; `revocation_evictions_total{kind}` registered; reload audit includes sweep counts; a test pins that a removed user cannot resume their durable session (admission-side); README documents the operational semantics. |

## Progress

<!-- status-table:0040 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0040-T1 | ✅ done | 2026-07-05 | "New hub types Admission { subject, method, cert_serial, protocol } and AuthMethod (Anonymous/Password/Token/Certificate/Enhanced): the server-side revocable facts a reload sweep re-checks — the broker retains facts about the admission, never replayable credentials. HubCommand::Attach and PendingAttach carry the Admission (subsuming the ADR 0031 owner subject); the Online entry keeps it. Capture: mqtt_auth::mtls::serial_from_cert extracts the leaf serial; conn::CertAdmission (identity + serial) replaces the bare identity through tls_admission / cert_admission, threaded through every listener (TCP-TLS, WSS, QUIC — one shared extraction) and handle_stream/run_framed; authenticate() returns the AuthMethod alongside the Identity (enhanced-auth exchanges record Enhanced); a proxied session's vouched identity carries no serial (the landing node holds the actual TLS session — documented on the field). Eviction: HubCommand::Evict { client, reason } -> Hub::evict queues DISCONNECT 0x87 (v5 only; v3.1.1 has no server DISCONNECT) then routes through detach(_, _, graceful=false), so the will publishes and session retention (ADR 0009), backlog spill, and stale-detach conn_id guards behave exactly as any ungraceful end; the queued DISCONNECT drains to the wire before the dropped outbound closes the writer. Evicting an offline client is a no-op. Mechanism only — nothing calls evict yet except the command. Tests: eviction_disconnects_the_target_and_leaves_others_undisturbed (v5 victim gets 0x87 then close, will reaches a bystander whose own flow continues, offline eviction no-op with the hub still serving), evicting_a_v311_client_closes_without_a_disconnect_packet. Workspace green (768 tests), clippy zero warnings. |
| 0040-T2 | ⬜ planned | — |  |
| 0040-T3 | ⬜ planned | — |  |
| 0040-T4 | ⬜ planned | — |  |
| 0040-T5 | ⬜ planned | — |  |
<!-- /status-table:0040 -->

## Changelog

- **2026-07-05** — T1 (admission metadata + eviction primitive) landed: connections now
  carry their revocable admission facts (subject, auth method, mTLS leaf serial,
  protocol) into the hub's online table, and the hub can terminate a named session —
  v5 clients are told why (DISCONNECT 0x87), v3.1.1 just closes, wills and session
  retention behave as for any ungraceful end. Mechanism only; the T2/T3 sweeps drive it
  next.
- **2026-07-05** — ADR proposed and delivery opened. Scope fixed by a live-state gap
  survey: (1) revoked client certs leave open TLS sessions untouched (the existing CRL
  test drops the connection *before* reloading); (2) removed password users keep their
  open connections (auth runs once at CONNECT); (3) tightened subscribe-ACLs grandfather
  existing subscriptions (fan-out performs no authz); (4) established peer links survive
  cluster-cert revocation (peer TLS built once at startup; ADR 0032 deferred its reload);
  (5) gossip-plane revocation (ADR 0022 T7) is already per-datagram — the model to match;
  (6) durable sessions of removed identities stay inert until expiry (kept deliberately —
  see ADR §5); (7) no audit/metric distinguishes revocation reaching live state. Ordering:
  T1 (mechanism) → T2/T3 (client-plane sweeps, independently landable) → T4 (peer plane)
  → T5 (closure).
