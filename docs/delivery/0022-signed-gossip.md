---
adr: "0022"
title: Per-node signed gossip (authenticated SWIM identity)
adr_status: Accepted
tasks:
  - id: 0022-P1
    title: Crypto core — load node signing key; sign payload; verify cert chains to CA + extract CN + verify signature
    status: done
    date: 2026-06-22
    evidence: mqtt-auth/src/signed_gossip.rs; ecdsa_p256_sign_then_verify_returns_the_cn; ed25519_sign_then_verify_roundtrips; a_signature_from_another_key_is_rejected; a_cert_not_chaining_to_the_ca_is_rejected
  - id: 0022-P2
    title: Wire format v2 in swim_auth — optional signer/verifier, seal/open, KAT pinning, v1 still understood
    status: done
    date: 2026-06-22
    evidence: swim_auth.rs v2 seal/open + GossipSign/GossipVerify; signed_seal_open_roundtrips_and_returns_the_identity; v2_body_framing_is_pinned; require_signed_rejects_an_unsigned_v1_datagram; tampering_any_v2_byte_is_rejected_by_the_hmac; v1 sealed_wire_format_matches_known_answer still passes
  - id: 0022-P3
    title: Driver binds identity — open returns authenticated CN; swim_driver enforces CN == SWIM from
    status: in-progress
    notes: swim_driver drops a datagram whose authenticated CN != msg.from; end-to-end forged-from proof lands with P5
  - id: 0022-P4
    title: mqttd wiring — retain CA/cert/key material, build signer/verifier, MQTTD_SWIM_SIGNED mode + startup guards
    status: planned
  - id: 0022-P5
    title: Over-the-wire integration test — signed gossip accepted; forged from rejected; prefer-mode accepts v1
    status: planned
  - id: 0022-T6
    title: Cert caching by fingerprint (send full cert periodically, fingerprint otherwise) to shrink datagrams
    status: deferred
    notes: size optimisation only; inline self-contained certs are correct and bootstrap-safe, just larger
  - id: 0022-T7
    title: Certificate expiry / revocation handling for gossip certs
    status: deferred
    notes: same deferred concern as peer-bus mTLS (ADR 0002); a CA-chained cert is trusted for gossip until revocation lands cluster-wide
---

# Delivery — ADR 0022: Per-node signed gossip

Decision: [docs/adr/0022-signed-gossip.md](../adr/0022-signed-gossip.md).

This is the secure realisation of the goal ADR 0003 deferred as "use the cluster PKI
instead of a second secret" (`0003-T9`). It adds per-node signatures **on top of** the
shared-key HMAC, so a datagram must satisfy both. Correctness-critical security code: every
phase lands test-first, with known-answer tests pinning the wire/sign formats and
adversarial tests for each forgery vector.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0022-P1** Crypto core | Load an ECDSA/Ed25519 signing key from PEM; `sign(payload)`; `verify(ca_der, cert_der, payload, sig)` returns the cert CN iff the cert chains to the CA **and** the signature verifies. KATs pin signing; adversarial tests cover wrong-CA, tampered payload, swapped key. |
| **0022-P2** Wire v2 | `SwimAuth` takes an optional signer (seal) and verifier (open); v2 layout `[2][HMAC32][cert_len][cert][sig_len][sig][payload]`; HMAC covers cert+sig+payload; a KAT pins the v2 layout; v1 still opens; every tampered field is rejected. |
| **0022-P3** Identity binding | `open` surfaces the authenticated CN; `swim_driver` drops a datagram whose authenticated CN ≠ the decoded SWIM `from` (the anti-impersonation check). |
| **0022-P4** Wiring | `PeerTls` retains the CA/leaf DER + key; `mqttd` builds the signer/verifier and selects `MQTTD_SWIM_SIGNED` = `require`/`prefer`/`off`; `require` without TLS material is a startup error; transitional/insecure modes loudly logged. |
| **0022-P5** Integration | Two in-process nodes over UDP exchange signed gossip and converge; a datagram with a forged `from` (valid cert+sig for node A claiming to be node B) is rejected; `prefer` mode still accepts a v1 (shared-key-only) peer during rollout. |

## Progress

<!-- status-table:0022 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0022-P1 | ✅ done | 2026-06-22 | mqtt-auth/src/signed_gossip.rs; ecdsa_p256_sign_then_verify_returns_the_cn; ed25519_sign_then_verify_roundtrips; a_signature_from_another_key_is_rejected; a_cert_not_chaining_to_the_ca_is_rejected |
| 0022-P2 | ✅ done | 2026-06-22 | swim_auth.rs v2 seal/open + GossipSign/GossipVerify; signed_seal_open_roundtrips_and_returns_the_identity; v2_body_framing_is_pinned; require_signed_rejects_an_unsigned_v1_datagram; tampering_any_v2_byte_is_rejected_by_the_hmac; v1 sealed_wire_format_matches_known_answer still passes |
| 0022-P3 | 🚧 in-progress | — | swim_driver drops a datagram whose authenticated CN != msg.from; end-to-end forged-from proof lands with P5 |
| 0022-P4 | ⬜ planned | — |  |
| 0022-P5 | ⬜ planned | — |  |
| 0022-T6 | 💤 deferred | — | size optimisation only; inline self-contained certs are correct and bootstrap-safe, just larger |
| 0022-T7 | 💤 deferred | — | same deferred concern as peer-bus mTLS (ADR 0002); a CA-chained cert is trusted for gossip until revocation lands cluster-wide |
<!-- /status-table:0022 -->

## Changelog

- **2026-06-22** — ADR accepted; phased plan recorded. Supersedes the (cryptographically
  unsound) `0003-T9` "derive the key from CA material" idea, which is now cut.
