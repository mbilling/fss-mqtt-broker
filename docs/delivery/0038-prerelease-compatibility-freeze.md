---
adr: "0038"
title: Pre-release compatibility freeze (versioned wire, stamped schemas, final codecs)
adr_status: Proposed
tasks:
  - id: 0038-T1
    title: Peer-bus version negotiation — Hello carries proto_min..proto_max, disjoint ranges reject the link loudly; Hello/ProxyHello encodings frozen
    status: done
    date: 2026-07-04
    evidence: "PeerMessage::Hello gains proto_min/proto_max (PROTO_MIN = PROTO_MAX = 1); pub fn negotiate_proto picks the newest version both ranges can speak (min of maxes, valid only if >= both mins) or None for disjoint ranges. Both handshake sides enforce it in mqttd::peer::handle via proto_compatible: the ACCEPT side rejects BEFORE announcing itself (an incompatible build gets a clean close, not half a handshake); the DIAL side rejects on the reply. Rejection is loud (warn with both ranges) and fail-closed — the link never registers with the hub. Hello and ProxyHello are documented as FROZEN frames (the bootstrap any two future builds must exchange to discover disagreement); all other frames evolve behind a proto_max bump, with the negotiated link version defined as min(proto_max_a, proto_max_b). Tests: negotiate_proto unit matrix (identical, overlapping, touching, disjoint both directions, own-constants sanity); integration over a real TCP listener (an_incompatible_peer_protocol_range_is_rejected_at_hello): a doctored Hello announcing 99..99 gets no Hello reply and a clean close, while the same handshake at the build's own range completes; wire roundtrip updated; whole workspace green (756 tests) proving same-version links are unaffected."
  - id: 0038-T2
    title: Schema-version stamps — shared redb schema gate (stamp fresh, pass equal, fail closed on mismatch) adopted by sessions/replicas/lease/retained stores
    status: planned
  - id: 0038-T3
    title: Retained MQTT 5 fidelity — app properties through the durable record codec, RetainedCommit/Update/Snapshot frames, and the persistent retained store
    status: planned
  - id: 0038-T4
    title: Wire-shape finalization — named serde structs for multi-field entries; frozen-vs-versioned frame inventory recorded
    status: planned
---

# Delivery — ADR 0038: Pre-release compatibility freeze

Decision: [docs/adr/0038-prerelease-compatibility-freeze.md](../adr/0038-prerelease-compatibility-freeze.md).

The last free window: no deployments exist, so wire frames and disk schemas can still
change in place. This delivery adds the machinery that makes post-release evolution
possible (version negotiation, schema markers) and pays the remaining codec debt
(retained app-properties) before the formats freeze at the first release.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0038-T1** Version negotiation | `Hello` carries `proto_min..proto_max`; a link whose ranges are disjoint is rejected before registration, loudly, on both accept and dial sides; same-version links are unaffected (whole suite green). `Hello`/`ProxyHello` are documented as frozen. |
| **0038-T2** Schema stamps | Every redb store (`sessions`, `replicas`, `lease`, `retained`) stamps a schema version on first open, passes on equal, and **refuses to open** on mismatch with an error naming found-vs-expected. Covered per store by fresh/reopen/mismatch tests. |
| **0038-T3** Retained fidelity | A retained publish with MQTT 5 properties (Content-Type, User Properties, …) replays with those properties from **any** node's cache — committed record, commit fan-out, and token back-fill all carry them; the persistent retained store round-trips them across restart. |
| **0038-T4** Wire shapes | Multi-field wire entries are named structs; the delivery doc records the frame inventory (frozen: `Hello`, `ProxyHello`; versioned: all others). |

## Progress

<!-- status-table:0038 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0038-T1 | ✅ done | 2026-07-04 | "PeerMessage::Hello gains proto_min/proto_max (PROTO_MIN = PROTO_MAX = 1); pub fn negotiate_proto picks the newest version both ranges can speak (min of maxes, valid only if >= both mins) or None for disjoint ranges. Both handshake sides enforce it in mqttd::peer::handle via proto_compatible: the ACCEPT side rejects BEFORE announcing itself (an incompatible build gets a clean close, not half a handshake); the DIAL side rejects on the reply. Rejection is loud (warn with both ranges) and fail-closed — the link never registers with the hub. Hello and ProxyHello are documented as FROZEN frames (the bootstrap any two future builds must exchange to discover disagreement); all other frames evolve behind a proto_max bump, with the negotiated link version defined as min(proto_max_a, proto_max_b). Tests: negotiate_proto unit matrix (identical, overlapping, touching, disjoint both directions, own-constants sanity); integration over a real TCP listener (an_incompatible_peer_protocol_range_is_rejected_at_hello): a doctored Hello announcing 99..99 gets no Hello reply and a clean close, while the same handshake at the build's own range completes; wire roundtrip updated; whole workspace green (756 tests) proving same-version links are unaffected." |
| 0038-T2 | ⬜ planned | — |  |
| 0038-T3 | ⬜ planned | — |  |
| 0038-T4 | ⬜ planned | — |  |
<!-- /status-table:0038 -->

## Changelog

- **2026-07-04** — T1 (version negotiation) landed: the peer-bus handshake now
  announces and checks a protocol range, failing closed (loudly) on disjoint ranges —
  the machinery a rolling upgrade needs to *detect* divergence is in the field before
  the first release can create divergence. `Hello`/`ProxyHello` are frozen from here on.
- **2026-07-04** — ADR proposed and delivery opened. Part of the pre-release plan
  (areas: ① this freeze, ② the "revocation reaches live state" security bundle,
  ③ resource governance, ④ a durable-plane stress/simulation harness — each of ②–④
  gets its own ADR when its work starts). This one goes first because its window
  closes at the first release: everything here is an in-place edit today and a
  migration-plus-mixed-cluster problem afterwards.
