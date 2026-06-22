# ADR 0004 — Identity model: mTLS Common Name first, deny by default

- **Status:** Accepted
- **Date:** 2026-06-12
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0004-identity-and-authentication.md](../delivery/0004-identity-and-authentication.md) — plan, progress, and changelog
- **Related:** ADR 0002 (transport security), Capability Plan §3, `mqtt-auth`

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
- **`%i`** substitutes the identity subject in rule topics at evaluation time.
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
- **Known limitation:** ACL enforcement is subscription-time only; a *delivery-
  time* check in the hub (needed if policies ever change under live
  subscriptions) is deferred along with hot reload. `%c` (client-id)
  substitution in ACL patterns is deferred until the `Authorizer` trait carries
  the client id. Full OIDC discovery / JWKS rotation, SAN-based identity
  selection, per-listener auth policies, and MQTT 5 enhanced auth are likewise
  deferred.
