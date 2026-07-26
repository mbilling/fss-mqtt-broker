# ADR 0050 — OIDC-integrated token authentication (discovery, JWKS rotation, proven against a real IdP)

- **Status:** Accepted
- **Date:** 2026-07-24 (accepted 2026-07-24)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0050-oidc-token-authentication.md](../delivery/0050-oidc-token-authentication.md) — plan, progress, and changelog
- **Related:** [ADR 0004](0004-identity-and-authentication.md) (whose deferred T9 this
  supersedes — the static-key `TokenAuthenticator` this builds on), [ADR 0013](0013-enhanced-auth.md)
  (the MQTT 5 AUTH exchange a token can also ride in; this ADR is transport-agnostic),
  [ADR 0017](0017-durable-attach-readiness.md) (the fail-closed philosophy the JWKS-outage
  policy follows), [ADR 0032](0032-hot-reloadable-security-policy.md) (rotation without
  restart is the same operational promise, here for third-party trust material)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0050-oidc-token-authentication.md).

## Context

The `TokenAuthenticator` (ADR 0004 step 6) validates JWTs against a **single static key**
(HS256 shared secret or one RS256 public key) with optional `iss`/`aud` checks. That is fine
for a closed deployment, but any real identity-provider integration (Keycloak, Auth0, Entra,
Dex, …) rotates its signing keys as a matter of hygiene — and with a static key, every
rotation is a broker reconfiguration, and every grace-period overlap (two live keys) is
unsupported. ADR 0004 deferred this as T9; it is a subsystem decision, not a config option:
the broker would be **fetching trust material over the network at runtime**, which brings a
new failure-mode policy, new attack surface, and new supply chain — so it gets its own record.

A hard requirement, set at proposal time: **this feature does not ship on unit tests alone.
It must be proven against a real OIDC provider, in CI, including a forced key rotation.**
A mocked JWKS endpoint tests our parsing; it does not test discovery quirks, JWKS document
shape in the wild, `kid` behaviour across rotation, or clock skew against an IdP's clock.

## Decision

Extend token authentication with an **OIDC mode**: configured with an **issuer URL** only,
the broker discovers the JWKS endpoint, fetches and caches the key set, selects keys by
`kid`, and follows rotation — no restart, no reconfiguration.

### 1. Discovery and key sourcing

`MQTTD_OIDC_ISSUER=https://idp.example/realms/iot` enables the mode. The broker fetches
`<issuer>/.well-known/openid-configuration`, reads `jwks_uri`, and loads the JWKS. Keys are
selected per token by `kid`. The static-key mode remains for closed deployments; the two are
not mixed on one authenticator (no silent fallback from OIDC to a static key).

### 2. Rotation, caching, and the unknown-`kid` refresh

The JWKS is cached with a TTL (`MQTTD_OIDC_JWKS_REFRESH`, default 5 min) and refreshed in the
background. A token bearing an **unknown `kid`** triggers one immediate, debounced refetch —
that is how a just-rotated IdP is picked up within seconds rather than a TTL. Old keys keep
validating as long as the IdP still publishes them (the standard overlap window); keys the
IdP has withdrawn stop validating on the next refresh. There is no per-connection fetch: the
hot path only ever reads the in-memory key set.

### 3. Failure policy: last-known-good, then fail closed

If the JWKS endpoint becomes unreachable, the broker keeps validating with the
**last-known-good key set** for a bounded staleness window (`MQTTD_OIDC_MAX_STALE`, default
24 h) — an IdP outage must not sever the whole fleet. Beyond the window, token authentication
**fails closed** (connects rejected, loudly logged), per the ADR 0017 philosophy: never
degrade into accepting what can no longer be verified. Startup with no reachable IdP and no
cached keys fails closed immediately.

### 4. Validation hardening

OIDC mode requires `iss` (must equal the configured issuer) and `aud`; unlike static mode
they are not optional. The algorithm allow-list is asymmetric-only (`RS256`, `ES256`) —
`HS*` is refused in OIDC mode (a public JWKS must never feed an HMAC verify: the classic
key-confusion attack), as is `alg=none`. `exp`/`nbf` are enforced with bounded clock skew.
The issuer URL must be `https://`; a loudly-logged `MQTTD_OIDC_ALLOW_HTTP=1` exists for
tests and closed networks, following the codebase's established INSECURE-warning idiom.

### 5. Supply chain

No new TLS or HTTP stack: the fetcher reuses the rustls-based HTTP client already in the
tree (the OTLP exporter's), and JWKS/JWT handling stays on the already-vetted `jsonwebtoken`.
A new full-featured OIDC client crate is explicitly rejected — discovery + JWKS is two GET
requests and a JSON schema; the attack surface budget goes to the validation logic, not to
a dependency tree.

### 6. The acceptance bar: a real IdP in CI, rotation included

The integration test — a first-class deliverable, not an afterthought — runs a **real OIDC
provider** (Keycloak, containerized and pinned) in CI and proves end to end:

1. broker discovers + loads keys from the live IdP; a client presenting an IdP-minted token
   connects (and its claims map to the session identity);
2. a bad-audience / expired / wrong-issuer token is rejected;
3. **keys are rotated in the IdP mid-test** (new active key via the admin API): a token
   signed with the new `kid` is accepted via the unknown-`kid` refresh path, without restart;
4. after the old key is withdrawn from the JWKS, tokens signed with it are rejected;
5. with the IdP stopped, authentication keeps working on cached keys, and with the staleness
   window forced to zero it fails closed.

Keycloak is chosen over lighter IdPs (Dex) precisely because its admin API can force a key
rotation on demand — requirement 3 is the point of the test. The job runs in the nightly
tier (ADR 0044 P4), like the other heavy-infrastructure suites; the deterministic unit tests
for cache/refresh/debounce logic run per-PR.

## Consequences

- Key rotation becomes the IdP's routine, not the broker operator's incident. Enterprise
  IdP integration works out of the box with an issuer URL.
- The broker gains a runtime network dependency for trust material — bounded by the
  last-known-good window and the fail-closed floor, and it never touches the hot path.
- CI gains a Keycloak container in the nightly tier — heavier infrastructure, justified by
  the acceptance bar: without it we would be claiming IdP compatibility we never exercised.
- The static-key mode stays for closed deployments; nothing existing changes behaviour.

## Alternatives considered

- **Amend ADR 0004 instead of a new record:** rejected — 0004 is Accepted and decision-only;
  runtime trust-material fetching is a new decision space (failure policy, attack surface,
  CI infra), not an erratum to the identity model.
- **A mock JWKS server for the integration test:** rejected as the acceptance bar — it tests
  our own assumptions back at us. Kept only as the fast per-PR unit harness.
- **A full OIDC client crate:** rejected (supply chain; we need two GETs and strict JWT
  validation, not a browser flow).
- **Fail open on JWKS outage (accept while unverifiable):** rejected outright — inverted
  security. **Fail closed immediately (no staleness window):** rejected too; an IdP blip
  severing an entire device fleet is an availability own-goal the bounded window prevents.
