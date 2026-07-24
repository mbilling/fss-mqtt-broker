---
adr: "0050"
title: "OIDC-integrated token authentication (discovery, JWKS rotation, proven against a real IdP)"
adr_status: Accepted
tasks:
  - id: 0050-T1
    title: Discovery + JWKS fetch — issuer URL -> .well-known/openid-configuration -> jwks_uri -> key set, over the in-tree rustls HTTP client; https-only (loud MQTTD_OIDC_ALLOW_HTTP override); no new OIDC/HTTP dependency
    status: planned
  - id: 0050-T2
    title: Rotation machinery — kid-selected keys, TTL background refresh (MQTTD_OIDC_JWKS_REFRESH), debounced unknown-kid immediate refetch, last-known-good cache with bounded staleness (MQTTD_OIDC_MAX_STALE) then fail-closed; deterministic per-PR unit tests for cache/refresh/debounce/staleness
    status: planned
  - id: 0050-T3
    title: Validation hardening + wiring — OIDC mode on TokenAuthenticator with required iss/aud, asymmetric-only algorithm allow-list (RS256/ES256, no HS* against a public JWKS, no none), bounded clock skew; composes with CONNECT-password and MQTT5 AUTH (ADR 0013) token transport
    status: planned
  - id: 0050-T4
    title: "THE ACCEPTANCE BAR — real-IdP integration test in CI (nightly tier): pinned Keycloak container; IdP-minted token connects and maps to session identity; bad aud/iss/expiry rejected; key ROTATED mid-test via the admin API and a new-kid token accepted without restart; withdrawn-key tokens rejected; IdP down -> cached keys keep working; staleness forced to zero -> fail closed"
    status: planned
  - id: 0050-T5
    title: Docs + ops — README auth section, env reference, failure-policy runbook note; ADR 0004 T9 marked superseded by this record
    status: planned
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
| 0050-T1 | ⬜ planned | — |  |
| 0050-T2 | ⬜ planned | — |  |
| 0050-T3 | ⬜ planned | — |  |
| 0050-T4 | ⬜ planned | — |  |
| 0050-T5 | ⬜ planned | — |  |
<!-- /status-table:0050 -->

## Changelog

- **2026-07-24** — ADR proposed with the real-IdP acceptance bar set at proposal time
  (integration against a live OIDC provider with forced rotation is a merge requirement for
  the feature, decided before any implementation exists to argue with it).
