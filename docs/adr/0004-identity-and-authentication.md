# ADR 0004 — Identity model: mTLS Common Name first, deny by default

- **Status:** Accepted
- **Date:** 2026-06-12
- **Deciders:** project maintainers
- **Related:** ADR 0002 (transport security), Capability Plan §3, `mqtt-auth`

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
   without touching the connection gate.

## Consequences

- A default-configured TLS listener with a client CA serves only
  certificate-authenticated clients; the plaintext listener is useless without
  the explicit anonymous opt-in. Config claims and enforcement now agree.
- The CN extraction is reusable on the cluster bus: binding peer `Hello`
  node ids to peer-certificate CNs (ADR 0002's deferred item) is now one small
  step.
- `conn::handle` remains a permissive anonymous shim for the integration test
  suites; production listeners do not use it.

## Deferred (the rest of the auth plan)

- **Step 3:** file-based topic ACL engine (deny-by-default, `%c`/`%i`
  substitution), enforced at SUBSCRIBE (0x80), PUBLISH (drop + audit), and the
  will topic at CONNECT.
- **Step 4:** audit-chain integration for auth/ACL events.
- **Step 5:** peer node-id ↔ certificate CN binding.
- **Step 6:** Argon2id password file, JWT/OIDC; MQTT 5 enhanced auth after the
  v5 codec.
- SAN-based identity selection; per-listener auth policies; hot ACL reload.
