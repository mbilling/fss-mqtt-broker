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
    evidence: swim_auth.rs v2 seal/open + GossipSign/GossipVerify; signed_seal_open_roundtrips_and_returns_the_identity; v2_body_framing_is_pinned; a_signed_node_rejects_an_unsigned_v1_datagram; tampering_any_v2_byte_is_rejected_by_the_hmac; v1 sealed_wire_format_matches_known_answer still passes
  - id: 0022-P3
    title: Driver binds identity — open returns authenticated CN; swim_driver enforces CN == SWIM from
    status: done
    date: 2026-06-22
    evidence: swim_driver.rs drops authenticated CN != msg.from; proven end-to-end by a_forged_sender_identity_is_rejected
  - id: 0022-P4
    title: mqttd wiring — retain CA/cert/key material, build signer/verifier, MQTTD_SWIM_SIGNED mode + startup guards
    status: done
    date: 2026-06-22
    evidence: main.rs apply_signed_gossip + NodeGossipSigner/CaGossipVerifier; PeerTls ca_der/cert_der/key_der; tls::first_cert_der/private_key_der; MQTTD_SWIM_SIGNED require/off (strict postures) with startup guards
  - id: 0022-P5
    title: Over-the-wire integration test — signed gossip accepted; forged from rejected; a signed node rejects an unsigned v1
    status: done
    date: 2026-06-22
    evidence: swim_cluster.rs signed_gossip_converges; a_forged_sender_identity_is_rejected (forged-from over real UDP); swim_auth a_signed_node_rejects_an_unsigned_v1_datagram
  - id: 0022-T6
    title: Cert caching by fingerprint (send full cert periodically, fingerprint otherwise) to shrink datagrams
    status: done
    date: 2026-07-02
    evidence: "v2/v3 bodies now carry a cert-ref instead of an always-inline certificate: the full leaf on PRIMING datagrams (Join/Sync — first-contact/full-state moments, selected by the driver from the message kind, seal/seal_sequenced gain a prime flag) and on every FULL_CERT_EVERY-th (16) send as a UDP-loss/restart recovery bound; a 32-byte SHA-256 fingerprint otherwise — routine signed gossip drops ~0.5 KiB per datagram and stays under a 1500-byte MTU. The receiver caches by fingerprint with ONLY fully-verified certificates admitted (bounded at 128, clear-and-reprime on overflow), and fingerprinting is pure wire compression: every datagram, fingerprint-form included, re-runs the complete verification (chain, validity, CRL, signature) against the cached DER, so T7 revocation applies to fingerprint datagrams immediately. A cache miss is the bounded recoverable drop reason cert-miss. Tests: a_primed_receiver_opens_a_fingerprint_datagram (incl. ~600B-cert size assertion), an_unprimed_receiver_misses_then_recovers, the_full_cert_recurs_periodically, priming_forces_the_full_certificate, the_fingerprint_path_verifies_and_sequences_like_the_full_path (v3 + full per-byte tamper sweep); v2/v3 framing KATs pin the cert-ref layout; the signed/sequenced over-UDP convergence suite now runs on fingerprint-form routine traffic."
  - id: 0022-T7
    title: Certificate expiry / revocation handling for gossip certs
    status: done
    date: 2026-07-02
    evidence: "signed_gossip::verify now checks the leaf's validity window (notBefore/notAfter at an injected epoch-seconds clock; an unrepresentable clock fails closed) and its serial against a cluster CRL. RevocationList::from_der parses the DER CRL, VERIFIES IT IS SIGNED BY THE CLUSTER CA (an unauthenticated CRL could revoke healthy nodes), and extracts revoked serials. MQTTD_PEER_TLS_CRL (requires the peer-TLS trio; bad CRL = startup error) loads it into a shared slot (reload::SwimCrlSlot) the CaGossipVerifier consults per datagram and the ADR 0032 Reloader swaps on SIGHUP / the ADR 0033 watcher (path added to watched_policy_paths) — revocation lands without restart, all-or-nothing with the rest of the policy. The ADR 0003-T6 drop counter gains bounded reasons expired/revoked (OpenReject). Tests: mqtt-auth an_expired_certificate_is_rejected, a_not_yet_valid_certificate_is_rejected, a_revoked_certificate_is_rejected, an_unlisted_certificate_passes_with_a_crl_loaded, a_crl_not_signed_by_the_cluster_ca_is_rejected_at_load, garbage_crl_bytes_do_not_panic; reload a_reload_swaps_the_gossip_crl_into_the_live_slot, a_bad_gossip_crl_rejects_the_whole_reload."
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
| **0022-P4** Wiring | `PeerTls` retains the CA/leaf DER + key; `mqttd` builds the signer/verifier and selects `MQTTD_SWIM_SIGNED` = `require`/`off` (strict postures); `require` without TLS material is a startup error; the insecure (`off`) mode loudly logged. |
| **0022-P5** Integration | Two in-process nodes over UDP exchange signed gossip and converge; a datagram with a forged `from` (valid cert+sig for node A claiming to be node B) is rejected; a signed (`require`) node rejects an unsigned v1 datagram (strict posture — no v1 coexistence). |

## Progress

<!-- status-table:0022 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0022-P1 | ✅ done | 2026-06-22 | mqtt-auth/src/signed_gossip.rs; ecdsa_p256_sign_then_verify_returns_the_cn; ed25519_sign_then_verify_roundtrips; a_signature_from_another_key_is_rejected; a_cert_not_chaining_to_the_ca_is_rejected |
| 0022-P2 | ✅ done | 2026-06-22 | swim_auth.rs v2 seal/open + GossipSign/GossipVerify; signed_seal_open_roundtrips_and_returns_the_identity; v2_body_framing_is_pinned; a_signed_node_rejects_an_unsigned_v1_datagram; tampering_any_v2_byte_is_rejected_by_the_hmac; v1 sealed_wire_format_matches_known_answer still passes |
| 0022-P3 | ✅ done | 2026-06-22 | swim_driver.rs drops authenticated CN != msg.from; proven end-to-end by a_forged_sender_identity_is_rejected |
| 0022-P4 | ✅ done | 2026-06-22 | main.rs apply_signed_gossip + NodeGossipSigner/CaGossipVerifier; PeerTls ca_der/cert_der/key_der; tls::first_cert_der/private_key_der; MQTTD_SWIM_SIGNED require/off (strict postures) with startup guards |
| 0022-P5 | ✅ done | 2026-06-22 | swim_cluster.rs signed_gossip_converges; a_forged_sender_identity_is_rejected (forged-from over real UDP); swim_auth a_signed_node_rejects_an_unsigned_v1_datagram |
| 0022-T6 | ✅ done | 2026-07-02 | "v2/v3 bodies now carry a cert-ref instead of an always-inline certificate: the full leaf on PRIMING datagrams (Join/Sync — first-contact/full-state moments, selected by the driver from the message kind, seal/seal_sequenced gain a prime flag) and on every FULL_CERT_EVERY-th (16) send as a UDP-loss/restart recovery bound; a 32-byte SHA-256 fingerprint otherwise — routine signed gossip drops ~0.5 KiB per datagram and stays under a 1500-byte MTU. The receiver caches by fingerprint with ONLY fully-verified certificates admitted (bounded at 128, clear-and-reprime on overflow), and fingerprinting is pure wire compression: every datagram, fingerprint-form included, re-runs the complete verification (chain, validity, CRL, signature) against the cached DER, so T7 revocation applies to fingerprint datagrams immediately. A cache miss is the bounded recoverable drop reason cert-miss. Tests: a_primed_receiver_opens_a_fingerprint_datagram (incl. ~600B-cert size assertion), an_unprimed_receiver_misses_then_recovers, the_full_cert_recurs_periodically, priming_forces_the_full_certificate, the_fingerprint_path_verifies_and_sequences_like_the_full_path (v3 + full per-byte tamper sweep); v2/v3 framing KATs pin the cert-ref layout; the signed/sequenced over-UDP convergence suite now runs on fingerprint-form routine traffic." |
| 0022-T7 | ✅ done | 2026-07-02 | "signed_gossip::verify now checks the leaf's validity window (notBefore/notAfter at an injected epoch-seconds clock; an unrepresentable clock fails closed) and its serial against a cluster CRL. RevocationList::from_der parses the DER CRL, VERIFIES IT IS SIGNED BY THE CLUSTER CA (an unauthenticated CRL could revoke healthy nodes), and extracts revoked serials. MQTTD_PEER_TLS_CRL (requires the peer-TLS trio; bad CRL = startup error) loads it into a shared slot (reload::SwimCrlSlot) the CaGossipVerifier consults per datagram and the ADR 0032 Reloader swaps on SIGHUP / the ADR 0033 watcher (path added to watched_policy_paths) — revocation lands without restart, all-or-nothing with the rest of the policy. The ADR 0003-T6 drop counter gains bounded reasons expired/revoked (OpenReject). Tests: mqtt-auth an_expired_certificate_is_rejected, a_not_yet_valid_certificate_is_rejected, a_revoked_certificate_is_rejected, an_unlisted_certificate_passes_with_a_crl_loaded, a_crl_not_signed_by_the_cluster_ca_is_rejected_at_load, garbage_crl_bytes_do_not_panic; reload a_reload_swaps_the_gossip_crl_into_the_live_slot, a_bad_gossip_crl_rejects_the_whole_reload." |
<!-- /status-table:0022 -->

## Changelog

- **2026-07-02** — T6 (certificate fingerprinting) landed, closing ADR 0022 completely
  (7/7). The always-inline leaf certificate becomes a cert-ref: full cert on Join/Sync
  priming datagrams and every 16th send; a 32-byte SHA-256 fingerprint otherwise — routine
  signed gossip sheds ~0.5 KiB/datagram and stays under a 1500-byte MTU (no UDP
  fragmentation). Pure wire compression, not a trust change: only fully-verified certs are
  cached, and every fingerprint-form datagram re-runs the complete verification (chain,
  validity, CRL, signature) against the cached DER; a cache miss is the bounded,
  recoverable `cert-miss` drop.
- **2026-07-02** — T7 (certificate expiry + revocation on the gossip plane) landed.
  `signed_gossip::verify` rejects a leaf outside its validity window and one whose serial
  is on the cluster CRL; the CRL itself must be **signed by the cluster CA** to load (an
  unauthenticated revocation list could deny service to healthy nodes). `MQTTD_PEER_TLS_CRL`
  wires it in, hot-reloadable through the ADR 0032/0033 validate-before-swap path — so
  publishing a new CRL evicts a compromised node's gossip on the next datagram, no restart.
  The drop counter gains bounded `expired`/`revoked` reasons. In the same change the
  certificate became the carrier for **CA-attested failure-domain labels** (ADR 0016 T6,
  `urn:fss:failure-domain:<label>` SAN URI) — see the 0016 delivery doc.
- **2026-06-30** — Pre-release cleanup: the transitional `prefer` rollout mode was **removed**.
  The mainline was never deployed to production, so the zero-downtime, node-by-node upgrade
  path (sign outgoing but still accept unsigned v1) was never needed. `MQTTD_SWIM_SIGNED` is
  now a strict `require`/`off` posture — a `require` node accepts only v2 — and defaults to
  `require` when both the shared key and cluster-bus TLS material are present. `SwimAuth::with_signing`
  drops its `require_signed` flag; the mixed-mode interop tests are replaced by strict-posture
  rejection tests (`a_signed_node_rejects_an_unsigned_v1_datagram`, `a_shared_key_node_rejects_a_signed_datagram`).
- **2026-06-22** — P1–P5 landed, test-first: the `mqtt-auth` crypto core (sign/verify, with
  the forgery vectors); wire format v2 + `GossipSign`/`GossipVerify` in `swim_auth` (v1 KAT
  still passes, so backward compatible); the driver's CN-to-`from` identity binding; the
  `mqttd` wiring with `MQTTD_SWIM_SIGNED` require/prefer/off and startup guards (the `prefer`
  rollout mode was later removed pre-release — see the 2026-06-30 entry); and the
  over-UDP integration proof that a forged sender identity is rejected. T6 (cert-caching
  size optimisation) and T7 (cert revocation/expiry) remain deferred.
- **2026-06-22** — ADR accepted; phased plan recorded. Supersedes the (cryptographically
  unsound) `0003-T9` "derive the key from CA material" idea, which is now cut.
