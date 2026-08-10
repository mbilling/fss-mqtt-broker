# ADR 0004 — Identity model: mTLS Common Name first, deny by default

- **Status:** Accepted
- **Date:** 2026-06-12
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0004-identity-and-authentication.md](../delivery/0004-identity-and-authentication.md) — plan, progress, and changelog
- **Related:** ADR 0002 (transport security), Capability Plan §3, `mqtt-auth`;
  later records that took over parts of this one's deferred list —
  [ADR 0013](0013-enhanced-authentication.md) (MQTT 5 enhanced auth),
  [ADR 0032](0032-hot-reloadable-security-policy.md) / [ADR 0033](0033-config-file-watch-reload.md)
  (hot policy reload), [ADR 0040](0040-revocation-reaches-live-state.md) (reload sweeps live
  subscriptions, in place of a delivery-time re-check),
  [ADR 0050](0050-oidc-token-authentication.md) (OIDC discovery + JWKS rotation)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0004-identity-and-authentication.md).

## Context

Through the transport-security milestone, any client that completed a TLS
handshake could publish and subscribe to anything: `allow_anonymous: false`
was validated in config and then never enforced. "Security is the product"
requires an identity model before an authorization model, and decisions about
*what an identity is* are hard to reverse once ACLs reference it.

## Decision

1. **The primary identity is the mTLS leaf certificate's Subject Common
   Name.** The TLS layer verifies the chain against the listener's client CA;
   `mqtt_auth::mtls::identity_from_cert` then maps the verified leaf to
   `Identity { subject: CN }`. Unparseable certificates, trailing DER garbage,
   and missing/empty/non-string CNs are all rejected (no panics on
   CA-controlled bytes). SAN-based identity is a future config option for PKI
   setups that leave CN empty.

2. **Client ID ≠ identity.** The MQTT client id is a session handle chosen by
   the client; the certificate CN is *who*. ACLs (next step) reference
   identity, with `%c` (client id) and `%i` (identity) substitution in topic
   patterns. A strict `client_id == CN` binding can become a per-listener
   policy flag later; it is not the default.

3. **Authentication is a gate at CONNECT, before the hub.** A rejected client
   never touches session state. Credentials are derived in priority order:
   TLS-verified certificate identity, else CONNECT username/password, else
   anonymous. Failure maps to MQTT 3.1.1 CONNACK codes: **0x04** (bad user
   name or password) for failed password credentials, **0x05** (not
   authorized) for everything else; then the connection closes.

4. **Deny by default.** The built-in `BasicAuthenticator` accepts certificate
   identities as-is, accepts anonymous only behind an explicit opt-in
   (`MQTTD_ALLOW_ANONYMOUS`, loudly logged as INSECURE), and refuses
   password/token credentials with `NotPermitted` until real verifiers
   (Argon2id, JWT) land. A verified certificate with no usable CN yields *no*
   identity and therefore falls under the anonymous policy.

5. **One pluggable seam.** Everything flows through the existing
   `Authenticator` trait, so Argon2id passwords, JWT/OIDC, and LDAP slot in
   without touching the connection gate. A `ChainAuthenticator` tries cert →
   password → token; each non-handling member abstains (`NotPermitted`) and the
   first real verdict is final.

### The topic ACL engine

A TOML policy file (`MQTTD_ACL_FILE`) evaluated per identity, action, and
topic; without a policy file authorization is **not enforced** and the broker
logs that loudly. Schema and full semantics live in `mqtt_auth::acl`'s module
docs; the load-bearing decisions:

- **Deny > allow > default(deny).** Rule order is irrelevant.
- **Asymmetric topic tests for subscriptions.** Allow rules use *coverage*
  (`mqtt_core::filter_covers`): granting `devices/+/state` does not admit a
  `devices/#` subscription. Deny rules use *overlap*
  (`mqtt_core::filters_overlap`): denying `secret/#` refuses any subscription
  that could receive a matching message, including `#` — broad filters cannot
  tunnel past denials. Publishes are concrete topics and use plain filter
  matching for both effects.
- **`$`-rooted topics mirror `topic_matches`:** wildcard-leading patterns
  neither cover nor overlap `$`-rooted filters.
- **Principals:** any-of `identities` globs (`*` only, byte-wise, literal
  otherwise) or any-of `groups`; both empty = everyone.
- **`%i`** substitutes the identity subject and **`%c`** the client id in rule topics at
  evaluation time. Both fail closed on an empty value or one carrying `/`, `+` or `#`:
  an allow then grants nothing, a deny refuses outright. See the T12 amendment below for
  why the two are not interchangeable.
- **Enforcement:** SUBSCRIBE → per-filter 0x80, denied filters never reach the
  hub (so retained replay is implicitly gated); PUBLISH → dropped but still
  acknowledged per `QoS` (3.1.1 has no negative PUBACK; not acking strands
  conforming publishers in retry), logged; will topic → 0x05 at CONNECT (a
  will is a deferred publish — refuse it before accepting the session).

### Auditing and peer binding

- **Audit trail.** The connection layer records `auth.success`, `auth.failure`,
  `acl.deny.publish`, `acl.deny.subscribe`, and `acl.deny.will` into an
  [`AuditSink`]. The production `AuditLog` hash-chains every event (tamper-evident
  head) and emits a structured `tracing` event (target `audit`). Failures are
  keyed by client id, never a credential — no secret reaches the log.
- **Peer node-id ↔ certificate CN binding.** On the cluster bus a peer's
  `Hello { node_id }` must equal its certificate's Subject CN, checked on both
  link directions before the tie-break. Closes the ADR 0002 hole where any
  cluster-cert holder could claim any node id. No binding on the plaintext
  (insecure) mesh.
- **Password and token verifiers.** `PasswordAuthenticator` (Argon2id,
  `username:phc-hash` file, identical error for unknown-user and wrong-password —
  no enumeration oracle) and `TokenAuthenticator` (JWT HS256 / RS256 with a static
  key, `exp`/`iss`/`aud` validation, subject from `sub`, groups from a configurable
  claim).

## Consequences

- A default-configured TLS listener with a client CA serves only
  certificate-authenticated clients; the plaintext listener is useless without
  the explicit anonymous opt-in. Config claims and enforcement now agree.
- The CN extraction is reusable on the cluster bus: binding peer `Hello`
  node ids to peer-certificate CNs (ADR 0002's deferred item) is now one small
  step.
- `conn::handle` remains a permissive anonymous shim for the integration test
  suites; production listeners do not use it.
- **Known limitation (as of 2026-06-12, superseded — see below):** ACL enforcement is
  subscription-time only; a *delivery-time* check in the hub (needed if policies ever
  change under live subscriptions) is deferred along with hot reload. `%c` (client-id)
  substitution in ACL patterns is deferred until the `Authorizer` trait carries
  the client id. Full OIDC discovery / JWKS rotation, SAN-based identity
  selection, per-listener auth policies, and MQTT 5 enhanced auth are likewise
  deferred.

### Amendment (2026-07-27): what the deferred list became

The paragraph above is kept for the record and is no longer accurate. Later records
took over most of it, and the two items that remain in this ADR's scope are being
built rather than deferred:

- **Hot ACL reload** — delivered by [ADR 0032](0032-hot-reloadable-security-policy.md)
  (validate-before-swap behind `watch` handles) and [ADR 0033](0033-config-file-watch-reload.md)
  (the file-watch trigger).
- **Delivery-time ACL re-check** — *not* built, on purpose. The requirement behind it (a
  tightened ACL must reach already-established subscriptions) is met by
  [ADR 0040](0040-revocation-reaches-live-state.md)'s reload-triggered grant sweep, which
  is O(live state) once per policy change instead of O(messages) forever, and which also
  reaches revoked certificates, removed users, and peer links. ADR 0040 records the
  trade-off in its alternatives.
- **OIDC discovery / JWKS rotation** — [ADR 0050](0050-oidc-token-authentication.md),
  proven against a live IdP. **MQTT 5 enhanced auth** —
  [ADR 0013](0013-enhanced-authentication.md).
- **SAN-based identity selection** and **`%c` substitution** stay here, as T11/T12 of this
  ADR's delivery: neither is a new decision space, they are the config option and the
  trait plumbing this record already anticipated in points 1 and 2.
- **Per-listener auth policies** remain deferred, and will need their own record: they
  are a change to the shape of the listener configuration (ADR 0046), not a flag.

### Amendment (2026-07-27): `%c` is a namespacing tool, not an isolation boundary (T12)

`%c` substitution now ships, and the `Authorizer` trait carries the `ClientId` at every
decision point — SUBSCRIBE, PUBLISH, the will check at CONNECT, and ADR 0040's grant
sweep, where each session's grants are re-checked under *its own* handle.

The thing worth recording is what `%c` is **not**. `%i` and `%c` look symmetric in a
policy file and are not:

| | `%i` | `%c` |
|---|---|---|
| Value | identity subject | client id |
| Chosen by | the **server** (verified cert field, password record, token claim) | the **client**, freely |
| Bounded by | authentication | nothing, by default |

ADR 0031's session-owner guard stops a client from taking over *another identity's*
session, but nothing stops it from claiming any unused id it likes. So a rule granting
`dev/%c/#` does not confine a principal to one namespace — it grants the union over
every id that principal could choose. `%c` separates a principal's *own* sessions (per
device under one fleet identity); it is not a tenant boundary.

It becomes one only in combination: a `connect` rule (ADR 0031 option B) fixes the set of
claimable ids, and the reachable `%c` values are then exactly what that rule admits. The
`acl` module documents the pairing, and a test pins it.

Two consequences follow, both implemented:

- **Substitution fails closed per placeholder.** An empty value, or one carrying `/`,
  `+` or `#`, makes a pattern unusable: an allow grants nothing, a deny refuses outright.
  This matters more for `%c` than `%i` — the client id is fully attacker-chosen, and the
  broker accepts any non-empty UTF-8 id. A rule is only exposed to the placeholders it
  actually names, so a hostile id cannot spoil rules that never say `%c`.
- **`%c` is rejected in a `connect` rule's `clients` globs.** There it would match the
  client id against itself and allow every id — a rule written to constrain would
  silently permit. That is a policy-validation error, not a runtime surprise.
- **Substitution is a single left-to-right pass; substituted text is never rescanned.**
  Implementing it as two `replace` passes (`%i`, then `%c`) is the obvious approach and
  is wrong: a subject of literally `%c` — legal, since it carries no `/`, `+` or `#` —
  makes `dev/%i/#` expand to `dev/%c/#` and then to `dev/<client-id>/#`, so the *client*
  chooses the namespace of a rule that never mentioned `%c`. The flaw is also
  one-directional in whichever order the passes run, which is exactly the kind of
  asymmetry that survives review. In the single pass a `%` inside a substituted value is
  an ordinary character.

## Amendment (2026-08-10): the `Authenticator` trait is async (T15)

`Authenticator::authenticate` was synchronous, and every authenticator that ships is pure
computation — Argon2, a signature check, a certificate field — so nothing needed to
suspend. That made the sync signature look free. It was not: it made every *remote*
authenticator impossible to add without a workaround.

An HTTP hook, LDAP, or remote token introspection is a network call per CONNECT. Behind a
sync trait called from `async fn authenticate_connect`, the options were:

- **block a runtime worker** on every connection attempt — a handful of slow auth calls
  starve the executor that is also serving every other client;
- **`spawn_blocking` + a blocking HTTP client** — a thread per CONNECT, and a second HTTP
  stack in the dependency tree beside the one that already fetches JWKS;
- **a second, parallel `authenticate_async` method** with a default that delegates — no
  impl changes, but two methods that must not disagree, and a trait that documents its own
  workaround.

**The trait is now `async`.** The pure verifiers gained `async fn` and never await; the
chain awaits each member in turn. That is the honest shape: authentication *is* I/O in the
general case, and the type should say so rather than making the one implementation that
needs it pay for the shape chosen for the others.

### What it cost, stated rather than hidden

Thirty-eight unit tests of pure verifiers now need `#[tokio::test]` — a runtime they never
use. That is the price, it is visible in the diff, and it is smaller than either
alternative's price. Tests that exercise no authenticator (ACL parsing, mTLS field
extraction, gossip signatures) were left synchronous.

`async-trait` joins `mqtt-auth`'s dependencies. It is already in the tree via
`mqtt-storage`, `mqtt-cluster` and `mqttd`, so nothing new enters the supply chain.

### What did NOT change

`mqtt-auth` stays **I/O-free**. The trait can now express I/O; no implementation in this
crate performs any. The HTTP hook (T16) lives in `mqttd`, beside the JWKS fetcher that
already owns the `reqwest` dependency — the same split ADR 0050 uses, where the crate
holds a pure verifier and the binary does the fetching.

The `Authorizer` trait is untouched and stays synchronous: ACL evaluation is a local
decision over already-loaded policy, on the publish hot path, and has no reason to await.

### The contract an I/O-backed implementation takes on

`authenticate` runs on the CONNECT path and **the broker applies no timeout of its own**.
An implementation that performs I/O owns its own deadline, and must **fail closed** when it
expires: a hook that is unreachable has not authenticated anybody. This is stated on the
trait method, because it is the part a third-party implementer will otherwise get wrong.
