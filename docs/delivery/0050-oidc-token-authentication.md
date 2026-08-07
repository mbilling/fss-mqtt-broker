---
adr: "0050"
title: "OIDC-integrated token authentication (discovery, JWKS rotation, proven against a real IdP)"
adr_status: Accepted
tasks:
  - id: 0050-T1
    title: Discovery + JWKS fetch — issuer URL -> .well-known/openid-configuration -> jwks_uri -> key set, over the in-tree rustls HTTP client; https-only (loud MQTTD_OIDC_ALLOW_HTTP override); no new OIDC/HTTP dependency
    status: done
    date: 2026-07-26
    evidence: "mqttd::oidc::run_fetch_loop: discovery (<issuer>/.well-known/openid-configuration -> jwks_uri) then JWKS GET over the in-tree reqwest (rustls-no-provider on the process ring provider — reuses the OTLP stack, no new crate, no aws-lc beyond what OTLP already pulls). https enforced with the loud MQTTD_OIDC_ALLOW_HTTP test override; backoff discovery so the IdP may boot after the broker. Proven live end to end in T4 (broker logs 'OIDC discovery complete' + 'OIDC JWKS refreshed keys=1' against real Keycloak)."
  - id: 0050-T2
    title: Rotation machinery — kid-selected keys, TTL background refresh (MQTTD_OIDC_JWKS_REFRESH), debounced unknown-kid immediate refetch, last-known-good cache with bounded staleness (MQTTD_OIDC_MAX_STALE) then fail-closed; deterministic per-PR unit tests for cache/refresh/debounce/staleness
    status: done
    date: 2026-07-26
    evidence: "mqtt-auth::oidc::OidcAuthenticator: kid-selected keys, install_jwks validate-before-swap (a garbled/empty fetch never evicts last-known-good), bounded(1) refresh-hint channel (capacity IS the debounce), staleness beyond max_stale fails closed. Fetch loop does TTL refresh + hint-driven refetch floored by a 5s anti-stampede gap. 8 deterministic unit tests (cache/refresh/debounce/staleness/validate-before-swap/unknown-kid). Rotation proven live in T4: a mid-run new-kid token accepted WITHOUT restart."
  - id: 0050-T3
    title: Validation hardening + wiring — OIDC mode on TokenAuthenticator with required iss/aud, asymmetric-only algorithm allow-list (RS256/ES256, no HS* against a public JWKS, no none), bounded clock skew; composes with CONNECT-password and MQTT5 AUTH (ADR 0013) token transport
    status: done
    date: 2026-07-26
    evidence: "OIDC authenticate: required iss+aud, asymmetric-only allow-list enforced on the token HEADER before any key is consulted (alg=none and HS* die before key selection — the key-confusion guard), bounded clock skew. MQTTD_OIDC_* config wired via mqtt-config; built ONCE outside the reload closure so its key cache survives hot-reloads; mutually exclusive with the static MQTTD_JWT_* verifier. §0 wire->Credentials::Token bridge (JWT-in-password) makes it reachable from a real client — the piece that was silently missing (static TokenAuthenticator was unreachable, ADR 0004 T8 corrected). Proven through the real broker CONNECT handler (tests/auth.rs, chain-wrapped) and live in T4."
  - id: 0050-T4
    title: "THE ACCEPTANCE BAR — real-IdP integration test in CI (nightly tier): pinned Keycloak container; IdP-minted token connects and maps to session identity; bad aud/iss/expiry rejected; key ROTATED mid-test via the admin API and a new-kid token accepted without restart; withdrawn-key tokens rejected; IdP down -> cached keys keep working; staleness forced to zero -> fail closed"
    status: done
    date: 2026-07-26
    evidence: "scripts/oidc/run.sh drives the real mqttd binary against pinned Keycloak 26.0, JWT-in-password via the Mosquitto CLI (foreign client). Verified live 11/11 (was 7/7): IdP-minted token accepted; wrong-audience + garbage rejected; a fresh RSA signing key added via the admin API mid-run (new kid) and the new token accepted WITHOUT broker restart (unknown-kid refetch, confirmed by a live 'OIDC JWKS refreshed' log); cached keys keep validating after the IdP is stopped. Wired as the nightly 'oidc' job. The live run earned its keep — it caught ChainAuthenticator::handles_token defaulting to false (the broker wraps every authenticator in a chain, which the in-process test had bypassed); the per-PR tests/auth.rs bridge tests now wrap in a chain too. NOTE recorded 2026-08-07 in a delivery-record audit: the ACCEPTANCE BAR in this title named four checks the script did NOT perform — an EXPIRED token, a WRONG-ISSUER token, a withdrawn-key token, and staleness forced to zero failing closed — and the script's own header comment advertised two of them. All four are authentication REJECTIONS, so they were delivered rather than the title narrowed: exp and iss are tampered without re-signing (a broker that accepts them is not validating the claim at all), the withdrawn-key shape is a token correctly signed with a throwaway RSA key whose kid the IdP never published (it must not be rescued by the unknown-kid refetch path), and the zero-staleness case restarts the broker with MQTTD_OIDC_MAX_STALE=0 while the IdP is still down, requiring it to refuse the very token it accepted moments earlier on cached keys — the other half of the last-known-good policy."
  - id: 0050-T5
    title: Docs + ops — README auth section, env reference, failure-policy runbook note; ADR 0004 T9 marked superseded by this record
    status: done
    date: 2026-07-26
    evidence: "README security env table documents MQTTD_OIDC_* (issuer/audience/jwks_refresh/max_stale/groups_claim/allow_http), the JWT-in-password carriage, asymmetric-only + fail-closed policy, and the real-Keycloak proof. ADR 0004 T9 notes point here (superseded); ADR 0004 T8 delivery carries the reachability correction (token auth was unreachable before the ADR 0050 bridge)."
---

# Delivery — ADR 0050: OIDC-integrated token authentication

Decision: [docs/adr/0050-oidc-token-authentication.md](../adr/0050-oidc-token-authentication.md).

The static-key `TokenAuthenticator` (ADR 0004 step 6) grows an OIDC mode: issuer-URL
discovery, JWKS caching with `kid` selection, rotation followed live (TTL refresh + debounced
unknown-`kid` refetch), last-known-good survival of IdP outages with a bounded-staleness
fail-closed floor. **The feature is not done until it passes against a real, containerized
IdP in CI — including a forced mid-test key rotation** (T4); unit tests alone do not close
this ADR.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0050-T1** Discovery + fetch | Issuer URL alone configures the mode; discovery + JWKS load over the in-tree rustls HTTP stack; https enforced (loud override for tests); zero new HTTP/OIDC dependencies. |
| **0050-T2** Rotation machinery | `kid` selection; TTL background refresh; unknown-`kid` triggers one debounced refetch; IdP outage → last-known-good up to `MQTTD_OIDC_MAX_STALE`, then fail-closed; all cache/refresh/debounce/staleness logic deterministically unit-tested per-PR. |
| **0050-T3** Validation + wiring | OIDC mode requires `iss`+`aud`; RS256/ES256 only (no HS*/none); bounded clock skew; works for tokens in CONNECT password and in the MQTT5 AUTH exchange. |
| **0050-T4** Real-IdP proof | The five-point live sequence (accept, reject, **rotate mid-test**, withdraw, outage/fail-closed) passes against pinned Keycloak in the nightly tier. |
| **0050-T5** Docs + ops | README + env reference + runbook note; ADR 0004 T9 carries a superseded-by pointer. |

## Progress

<!-- status-table:0050 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0050-T1 | ✅ done | 2026-07-26 | "mqttd::oidc::run_fetch_loop: discovery (<issuer>/.well-known/openid-configuration -> jwks_uri) then JWKS GET over the in-tree reqwest (rustls-no-provider on the process ring provider — reuses the OTLP stack, no new crate, no aws-lc beyond what OTLP already pulls). https enforced with the loud MQTTD_OIDC_ALLOW_HTTP test override; backoff discovery so the IdP may boot after the broker. Proven live end to end in T4 (broker logs 'OIDC discovery complete' + 'OIDC JWKS refreshed keys=1' against real Keycloak)." |
| 0050-T2 | ✅ done | 2026-07-26 | "mqtt-auth::oidc::OidcAuthenticator: kid-selected keys, install_jwks validate-before-swap (a garbled/empty fetch never evicts last-known-good), bounded(1) refresh-hint channel (capacity IS the debounce), staleness beyond max_stale fails closed. Fetch loop does TTL refresh + hint-driven refetch floored by a 5s anti-stampede gap. 8 deterministic unit tests (cache/refresh/debounce/staleness/validate-before-swap/unknown-kid). Rotation proven live in T4: a mid-run new-kid token accepted WITHOUT restart." |
| 0050-T3 | ✅ done | 2026-07-26 | "OIDC authenticate: required iss+aud, asymmetric-only allow-list enforced on the token HEADER before any key is consulted (alg=none and HS* die before key selection — the key-confusion guard), bounded clock skew. MQTTD_OIDC_* config wired via mqtt-config; built ONCE outside the reload closure so its key cache survives hot-reloads; mutually exclusive with the static MQTTD_JWT_* verifier. §0 wire->Credentials::Token bridge (JWT-in-password) makes it reachable from a real client — the piece that was silently missing (static TokenAuthenticator was unreachable, ADR 0004 T8 corrected). Proven through the real broker CONNECT handler (tests/auth.rs, chain-wrapped) and live in T4." |
| 0050-T4 | ✅ done | 2026-07-26 | "scripts/oidc/run.sh drives the real mqttd binary against pinned Keycloak 26.0, JWT-in-password via the Mosquitto CLI (foreign client). Verified live 11/11 (was 7/7): IdP-minted token accepted; wrong-audience + garbage rejected; a fresh RSA signing key added via the admin API mid-run (new kid) and the new token accepted WITHOUT broker restart (unknown-kid refetch, confirmed by a live 'OIDC JWKS refreshed' log); cached keys keep validating after the IdP is stopped. Wired as the nightly 'oidc' job. The live run earned its keep — it caught ChainAuthenticator::handles_token defaulting to false (the broker wraps every authenticator in a chain, which the in-process test had bypassed); the per-PR tests/auth.rs bridge tests now wrap in a chain too. NOTE recorded 2026-08-07 in a delivery-record audit: the ACCEPTANCE BAR in this title named four checks the script did NOT perform — an EXPIRED token, a WRONG-ISSUER token, a withdrawn-key token, and staleness forced to zero failing closed — and the script's own header comment advertised two of them. All four are authentication REJECTIONS, so they were delivered rather than the title narrowed: exp and iss are tampered without re-signing (a broker that accepts them is not validating the claim at all), the withdrawn-key shape is a token correctly signed with a throwaway RSA key whose kid the IdP never published (it must not be rescued by the unknown-kid refetch path), and the zero-staleness case restarts the broker with MQTTD_OIDC_MAX_STALE=0 while the IdP is still down, requiring it to refuse the very token it accepted moments earlier on cached keys — the other half of the last-known-good policy." |
| 0050-T5 | ✅ done | 2026-07-26 | "README security env table documents MQTTD_OIDC_* (issuer/audience/jwks_refresh/max_stale/groups_claim/allow_http), the JWT-in-password carriage, asymmetric-only + fail-closed policy, and the real-Keycloak proof. ADR 0004 T9 notes point here (superseded); ADR 0004 T8 delivery carries the reachability correction (token auth was unreachable before the ADR 0050 bridge)." |
<!-- /status-table:0050 -->

## Changelog

- **2026-07-24** — ADR proposed with the real-IdP acceptance bar set at proposal time
  (integration against a live OIDC provider with forced rotation is a merge requirement for
  the feature, decided before any implementation exists to argue with it).
