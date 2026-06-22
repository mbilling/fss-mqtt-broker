---
adr: "0003"
title: "Gossip-plane authentication: keyed MAC on SWIM datagrams"
adr_status: Accepted
tasks:
  - id: 0003-T1
    title: HMAC-SHA256 tag on every SWIM datagram, framed [version][32-byte tag][payload], verify-before-decode at the I/O boundary
    status: done
    date: 2026-06-11
    evidence: swim_auth.rs SwimAuth::seal/open; sealed_wire_format_matches_known_answer; any_flipped_bit_is_rejected
  - id: 0003-T2
    title: Pure swim module stays crypto-free; sealing/opening lives in swim_auth / swim_driver
    status: done
    date: 2026-06-11
    evidence: swim_driver.rs run() opens before bincode::deserialize::<Message>; swim.rs holds no crypto
  - id: 0003-T3
    title: ring HMAC-SHA256 reused from the rustls tree (no new dependency, no BLAKE3)
    status: done
    date: 2026-06-11
    evidence: mqtt-cluster/Cargo.toml ring workspace dep; no blake3 in Cargo.lock
  - id: 0003-T4
    title: Key provisioning via MQTTD_SWIM_KEY (64-hex/32-byte); short or malformed keys are startup errors, unkeyed is possible but loudly INSECURE
    status: done
    date: 2026-06-11
    evidence: main.rs SwimAuth::from_hex_key(&hex)? + INSECURE warn; hex_key_parsing_enforces_exact_length
  - id: 0003-T5
    title: Replay accepted and bounded by SWIM incarnation supersession + Alive refutation
    status: done
    date: 2026-06-11
    evidence: swim.rs self-refutation (incarnation bump + Alive); refutes_suspicion_about_self; a_dead_member_is_not_revived_by_stale_higher_incarnation_gossip
  - id: 0003-T6
    title: Rejected-datagram metrics counter (operator signal for dropped gossip)
    status: deferred
    notes: drop path logs at debug only, no metric; lands with the observability phase (no gossip-reject counter in mqtt-observability)
  - id: 0003-T7
    title: Anti-replay window / per-peer nonces
    status: deferred
    notes: deferred until operational experience shows the transient-refutation cost matters; no nonce/window logic in swim_auth/swim_driver
  - id: 0003-T8
    title: Zero-downtime key rotation (dual-key acceptance window)
    status: deferred
    notes: SwimAuth holds a single key; rotation requires a cluster restart until a dual-key window is added
  - id: 0003-T9
    title: Derive the gossip key from cluster-CA material instead of a second secret
    status: cut
    notes: cryptographically unsound — the CA cert is public, so a key derived from it is not secret; the secure realisation (per-node signatures over the PKI) moved to ADR 0022
---

# Delivery — ADR 0003: Gossip-plane authentication: keyed MAC on SWIM datagrams

Decision: [docs/adr/0003-gossip-authentication.md](../adr/0003-gossip-authentication.md).

## Plan

The decision is a single mechanism — a keyed MAC sealing every SWIM datagram — plus a set
of deliberately deferred hardening items. Each carries a stable id used by commits, tests,
and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0003-T1** Keyed MAC | Every SWIM datagram carries an HMAC-SHA256 tag over the serialized message, framed `[version byte][32-byte tag][payload]`; receivers verify (constant-time) before decode and drop failures before the protocol state machine sees them. |
| **0003-T2** Crypto at the edge | The pure `swim` module stays crypto-free; sealing/opening lives at the I/O boundary in `swim_auth`/`swim_driver`. |
| **0003-T3** ring reuse | The MAC uses `ring`'s HMAC-SHA256, already in the tree via `rustls` — no new dependency and no BLAKE3 crate. |
| **0003-T4** Key provisioning | A 32-byte key arrives via `MQTTD_SWIM_KEY` (64 hex chars); short/malformed keys are startup errors with no weak-key mode; running unkeyed is possible and loudly logged INSECURE. |
| **0003-T5** Bounded replay | Replay is accepted but bounded: a replayed claim at/below current incarnation is superseded, and a replayed `Dead` triggers standard refutation (incarnation bump + `Alive` gossip). |
| **0003-T6** Reject counter | A rejected-datagram metrics counter gives operators the proper signal (rejections logged at `debug` to avoid a log-flooding lever). |
| **0003-T7** Anti-replay | A timestamp window or per-peer nonces prevent replay outright rather than merely bounding it. |
| **0003-T8** Key rotation | The cluster accepts old+new keys during a window so the gossip key rotates without downtime. |
| **0003-T9** CA-derived key | The gossip key is derived from cluster-CA material instead of being a second standalone secret. |

## Progress

<!-- status-table:0003 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0003-T1 | ✅ done | 2026-06-11 | swim_auth.rs SwimAuth::seal/open; sealed_wire_format_matches_known_answer; any_flipped_bit_is_rejected |
| 0003-T2 | ✅ done | 2026-06-11 | swim_driver.rs run() opens before bincode::deserialize::<Message>; swim.rs holds no crypto |
| 0003-T3 | ✅ done | 2026-06-11 | mqtt-cluster/Cargo.toml ring workspace dep; no blake3 in Cargo.lock |
| 0003-T4 | ✅ done | 2026-06-11 | main.rs SwimAuth::from_hex_key(&hex)? + INSECURE warn; hex_key_parsing_enforces_exact_length |
| 0003-T5 | ✅ done | 2026-06-11 | swim.rs self-refutation (incarnation bump + Alive); refutes_suspicion_about_self; a_dead_member_is_not_revived_by_stale_higher_incarnation_gossip |
| 0003-T6 | 💤 deferred | — | drop path logs at debug only, no metric; lands with the observability phase (no gossip-reject counter in mqtt-observability) |
| 0003-T7 | 💤 deferred | — | deferred until operational experience shows the transient-refutation cost matters; no nonce/window logic in swim_auth/swim_driver |
| 0003-T8 | 💤 deferred | — | SwimAuth holds a single key; rotation requires a cluster restart until a dual-key window is added |
| 0003-T9 | ✂️ cut | — | cryptographically unsound — the CA cert is public, so a key derived from it is not secret; the secure realisation (per-node signatures over the PKI) moved to ADR 0022 |
<!-- /status-table:0003 -->

## Changelog

- **2026-06-11** — Gossip authentication landed: HMAC-SHA256 seal/open at the I/O boundary
  with verify-before-decode (T1), the crypto-free pure `swim` module (T2), `ring` reused
  from the rustls tree (T3), and `MQTTD_SWIM_KEY` provisioning with startup validation and
  the loud-INSECURE unkeyed path (T4). Replay handling rides on SWIM's existing incarnation
  refutation (T5). Anti-replay windows, zero-downtime key rotation, CA-derived keys, and the
  rejected-datagram metric recorded as deliberate deferrals.
