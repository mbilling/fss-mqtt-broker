---
adr: "0013"
title: MQTT 5.0 enhanced authentication (AUTH exchange)
adr_status: Accepted
tasks:
  - id: 0013-T1
    title: EnhancedAuthenticator / AuthSession / AuthStep exchange abstraction in mqtt-auth
    status: done
    date: 2026-06-17
    evidence: mqtt-auth/src/enhanced.rs (EnhancedAuthenticator, AuthSession, AuthStep)
  - id: 0013-T2
    title: Connection-layer AUTH plumbing — method present runs exchange, absent falls through to single-shot
    status: done
    date: 2026-06-17
    evidence: conn.rs enhanced_auth + drive_auth_exchange
  - id: 0013-T3
    title: Reject CONNECT with unknown method (CONNACK 0x8C) and close on malformed exchange
    status: done
    date: 2026-06-17
    evidence: v5_enhanced_auth_unknown_method_is_rejected (CONNACK_V5_BAD_AUTH_METHOD 0x8C)
  - id: 0013-T4
    title: Reference HMAC-SHA256 challenge/response mechanism (ring nonce, constant-time verify)
    status: done
    date: 2026-06-17
    evidence: HmacChallengeAuthenticator; correct_proof_succeeds / wrong_secret_fails
  - id: 0013-T5
    title: Enhanced-auth connect happy/failure paths over the wire
    status: done
    date: 2026-06-17
    evidence: v5_enhanced_auth_hmac_succeeds / v5_enhanced_auth_wrong_proof_is_rejected
  - id: 0013-T6
    title: Client-initiated re-authentication in the serve loop (AUTH 0x19 reuses drive_auth_exchange, updates principal)
    status: done
    date: 2026-06-17
    evidence: reauthenticate(); v5_enhanced_auth_then_reauthentication
  - id: 0013-T7
    title: Re-auth edge enforcement — method must match (DISCONNECT 0x82), failure DISCONNECT 0x87
    status: done
    date: 2026-06-17
    evidence: v5_reauthentication_method_change_is_protocol_error / v5_reauthentication_wrong_proof_disconnects
  - id: 0013-T8
    title: Server-initiated re-auth (server sends AUTH 0x19 to demand re-authentication)
    status: deferred
    notes: ADR section 4 explicitly defers this — needs a trigger mechanism and interacts with the select-loop outbound path; only client-initiated re-auth is implemented (no server-side AUTH 0x19 send exists in conn.rs).
  - id: 0013-T9
    title: Dedicated per-round AUTH-exchange timeout
    status: done
    date: 2026-06-24
    evidence: "drive_auth_exchange wraps each round's reply read in a tokio timeout of WireLimits.auth_round_timeout (configurable via MQTTD_AUTH_TIMEOUT); a stalled round aborts the exchange rather than pinning the connection."
---

# Delivery — ADR 0013: MQTT 5.0 enhanced authentication (AUTH exchange)

Decision: [docs/adr/0013-enhanced-authentication.md](../adr/0013-enhanced-authentication.md).

## Plan

Despite the ADR header reading "design; phased", the enhanced-auth path is built end to
end: the trait pair, the connection plumbing, the reference HMAC mechanism, and
client-initiated re-authentication all ship with tests. The decision's four numbered
sections map to these tasks; only the explicitly-deferred server-initiated re-auth (§4)
and the no-per-round-timeout limit remain open.

| Task | Acceptance criterion |
|------|----------------------|
| **0013-T1** Exchange abstraction | `EnhancedAuthenticator` (registered by method name, `start()`), `AuthSession::step(client, data)`, and `AuthStep` (`Challenge`/`Success`/`Failure`) live in `mqtt-auth`, beside the untouched single-shot `Authenticator`. |
| **0013-T2** Connection plumbing | `run_framed` selects enhanced when CONNECT carries an Authentication Method, else falls through to single-shot `authenticate_connect`; the exchange loop sends AUTH `0x18` challenges and reads AUTH `0x18` replies. |
| **0013-T3** Method/exchange rejection | A method with no configured authenticator → CONNACK `0x8C`; a malformed exchange (wrong packet / mismatched method) closes the connection; a rejected client never reaches the hub. |
| **0013-T4** HMAC mechanism | `HmacChallengeAuthenticator`: subject in initial data, 32-byte `ring::rand` nonce, `HMAC-SHA256(secret, nonce)`, constant-time `ring::hmac::verify`; unknown subject still challenged before failing. |
| **0013-T5** Connect paths over the wire | A full v5 enhanced-auth CONNECT succeeds with the right proof and is rejected with a wrong proof, end to end. |
| **0013-T6** Client-initiated re-auth | An AUTH `0x19` on an established session starts a fresh exchange via the same `drive_auth_exchange`; success answers AUTH `0x00` and updates the principal used for ACL checks. |
| **0013-T7** Re-auth edges | Method change vs. CONNECT → DISCONNECT `0x82`; re-auth failure → DISCONNECT `0x87` and close. |
| **0013-T8** Server-initiated re-auth | The server sends AUTH `0x19` to demand the client re-authenticate (e.g. on credential expiry). |
| **0013-T9** Per-round timeout | A dedicated timeout bounds each round of the AUTH exchange rather than relying on the generic read surface. |

## Progress

<!-- status-table:0013 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0013-T1 | ✅ done | 2026-06-17 | mqtt-auth/src/enhanced.rs (EnhancedAuthenticator, AuthSession, AuthStep) |
| 0013-T2 | ✅ done | 2026-06-17 | conn.rs enhanced_auth + drive_auth_exchange |
| 0013-T3 | ✅ done | 2026-06-17 | v5_enhanced_auth_unknown_method_is_rejected (CONNACK_V5_BAD_AUTH_METHOD 0x8C) |
| 0013-T4 | ✅ done | 2026-06-17 | HmacChallengeAuthenticator; correct_proof_succeeds / wrong_secret_fails |
| 0013-T5 | ✅ done | 2026-06-17 | v5_enhanced_auth_hmac_succeeds / v5_enhanced_auth_wrong_proof_is_rejected |
| 0013-T6 | ✅ done | 2026-06-17 | reauthenticate(); v5_enhanced_auth_then_reauthentication |
| 0013-T7 | ✅ done | 2026-06-17 | v5_reauthentication_method_change_is_protocol_error / v5_reauthentication_wrong_proof_disconnects |
| 0013-T8 | 💤 deferred | — | ADR section 4 explicitly defers this — needs a trigger mechanism and interacts with the select-loop outbound path; only client-initiated re-auth is implemented (no server-side AUTH 0x19 send exists in conn.rs). |
| 0013-T9 | ✅ done | 2026-06-24 | "drive_auth_exchange wraps each round's reply read in a tokio timeout of WireLimits.auth_round_timeout (configurable via MQTTD_AUTH_TIMEOUT); a stalled round aborts the exchange rather than pinning the connection." |
<!-- /status-table:0013 -->

## Changelog

- **2026-06-17** — Enhanced authentication landed: exchange abstraction (T1), connection
  plumbing (T2), method/exchange rejection (T3), reference HMAC mechanism (T4), and
  wire-level connect proofs (T5). Client-initiated re-authentication (T6) and its edge
  enforcement (T7) landed with it. Server-initiated re-auth (T8, ADR §4) and a dedicated
  per-round timeout (T9) split out as deferred.
</content>
</invoke>
