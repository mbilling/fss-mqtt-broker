# ADR 0002 — Transport security: TLS 1.3 everywhere, mTLS on the cluster bus

- **Status:** Accepted
- **Date:** 2026-06-11
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0002-transport-security.md](../delivery/0002-transport-security.md) — plan, progress, and changelog
- **Related:** [Capability Plan](../CAPABILITY-PLAN.md) §3 (security), ADR 0001, `mqtt-net`

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0002-transport-security.md).

## Context

"Security is the product" (Capability Plan §1), yet through the routing spike
every byte the broker moves is plaintext: the client listener, the inter-node
peer links, and the SWIM gossip datagrams. Plaintext is opt-in and loudly
logged, but there is no secure mode to opt *into*. That inversion must end
before any further capability work: password auth over plaintext is theater,
and the cluster bus carries every cross-node message.

Decisions needed: TLS stack, supported protocol versions, how peers
authenticate each other, where TLS configuration is built, and what is
deliberately deferred.

## Decision

1. **`rustls` 0.23 with the `ring` provider.** Pure-Rust TLS, no OpenSSL CVE
   surface (Capability Plan §3). `ring` over the default `aws-lc-rs` because it
   builds without cmake/NASM toolchain requirements and its license terms pass
   our `cargo-deny` allow-list unmodified. Switching providers later is a
   one-line change confined to `mqtt-net`.

   > **Amended by [ADR 0053](0053-single-crypto-provider-aws-lc-rs.md)
   > (2026-08-04):** the provider is now `aws-lc-rs`. ring went into upstream
   > maintenance mode, and the OTLP exporter's reqwest chain had already pulled
   > aws-lc-rs into every build — the swap *removed* the second provider. (The
   > "one-line change" held for TLS; the direct `ring::` call sites in
   > mqtt-auth/mqtt-cluster took import renames.)

2. **TLS 1.3 only.** The plan says "TLS 1.2 opt-in only"; we go further and do
   not implement the 1.2 opt-in until a concrete deployment needs it. No
   protocol-version configuration surface exists until then — what isn't
   configurable can't be misconfigured.

3. **One module builds all TLS config: `mqtt_net::tls`.** PEM loading,
   server/acceptor and client/connector construction, and client-certificate
   verification policy live in one audited place. There is deliberately **no
   "skip verification" or "accept any certificate" code path** — not even for
   tests, which mint real throwaway CAs instead (`rcgen`, dev-dependency only).

4. **Client listener: TLS server, client certs optional per listener.**
   `require_client_cert` (mTLS) is governed by configuration whose default is
   `true` (`mqtt-config`); the env shims used until config-file loading lands
   make client-CA provisioning explicit. Identity-from-certificate (subject/SAN
   → MQTT identity) is Phase-2 auth work, not transport work.

5. **Cluster bus: mutual TLS, one cluster CA.** Peer links authenticate in
   *both* directions against a dedicated cluster CA: the listener requires a
   client certificate, the dialer verifies the server certificate, and both
   present leaf certs issued by the cluster CA. Possession of a cluster-CA-
   issued cert is what admits a node to the routing mesh. Client-facing and
   cluster-facing trust roots are separate inputs, so a client CA can never
   admit a node and vice versa.

## Consequences

- The broker finally has a secure mode; plaintext remains opt-in, loudly
  logged, and test-only in spirit.
- Cross-node routing (interest snapshots, forwarded publishes) is encrypted and
  mutually authenticated. A network position no longer suffices to join the
  mesh or read traffic.
- `ring`'s build simplicity costs FIPS availability (`aws-lc-rs` has a FIPS
  mode). Certified builds are a stated business line; revisit the provider when
  that work starts. *(Taken early — [ADR 0053](0053-single-crypto-provider-aws-lc-rs.md)
  moved to aws-lc-rs for maintenance reasons; FIPS is now a feature flag away.)*
- Tests minting real CAs keep the no-insecure-verifier invariant but make test
  setup slightly heavier (an in-test PKI helper).

## Deferred (tracked, deliberate)

- **Node-id ↔ certificate binding.** The peer `Hello` self-declared the node id;
  a valid cluster cert was required to speak at all, but any admitted node could
  claim any id. **Resolved by [ADR 0004](0004-identity-and-authentication.md)
  step 5**, which binds the node id to the peer certificate's Common Name.
- **SWIM gossip plane security.** UDP datagrams remain unauthenticated — an
  attacker who can reach the gossip port can still inject membership claims
  (and SWIM-driven routing makes `Dead` claims a remote kill switch). Needs a
  shared-key MAC or move onto the authenticated channel. **Resolved by
  [ADR 0003](0003-gossip-authentication.md).**
- **CRL / OCSP stapling, certificate rotation/reload** without dropping
  connections (pairs with hot-reloadable policy, Capability Plan §3).
- **WebSocket-over-TLS** listener (Phase 4).

## Amendment (2026-08-11): TLS 1.2 as a per-listener opt-in

TLS 1.2 was a deliberate non-feature "until a deployment demands it". The demand arrived
through the evaluation panel: real device fleets carry firmware that cannot negotiate 1.3,
and for them "TLS 1.3 only" reads as a hard migration blocker whose failure mode looks
like a network problem.

The decision holds its shape while gaining an escape hatch:

- **1.3 stays the only default.** Nothing changes for a broker that does not set the flag,
  and the README continues to advertise the default posture truthfully.
- **`MQTTD_TLS_ALLOW_TLS12` / `[tls].allow_tls12`** admits 1.2 clients on the
  client-facing TLS listener only. It is logged in the same REDUCED-POSTURE register as
  the other insecure modes, on every start, so it cannot be enabled and forgotten.
- **The cluster bus and QUIC never speak 1.2.** The bus builders do not take the flag;
  QUIC mandates 1.3 by protocol.
- The `tls12` rustls feature is now compiled (this file's earlier note said it was not) —
  compiled but unreachable without the flag, which is the honest new statement.
- Tested in both directions: a 1.2-only client is refused by a default listener and
  admitted (negotiating 1.2 on the wire) by an opted-in one.

### Hardening: opting into 1.2 does not opt into 1.2's exploits

What the opt-in admits is a **hardened** TLS 1.2, most of it structural to rustls and
pinned by test so a provider upgrade cannot quietly regress it:

- **ECDHE + AEAD suites only** — no CBC, no RC4/3DES, no static-RSA key exchange, so the
  POODLE / Lucky13 / Sweet32 / ROBOT exploit classes have no surface, and every session
  has forward secrecy. A test iterates the provider's 1.2 suites and fails if any
  non-ECDHE or non-AEAD suite ever appears.
- **No renegotiation, no compression, no export suites** — rustls does not implement
  them; there is nothing to misconfigure.
- **Extended Master Secret (RFC 7627) REQUIRED** — rustls' own default on this provider
  is *not* to require it, which would leave the triple-handshake attack surface open
  silently. The hardened posture refuses clients that cannot do EMS.

The one escape hatch is **`MQTTD_TLS_ALLOW_UNSAFE_TLS12_FEATURES` /
`[tls].allow_unsafe_tls12_features`** — off by default — which relaxes exactly the EMS
requirement for legacy firmware that predates RFC 7627. It is loudly logged on every
start while enabled, and it is a configuration **error** without `allow_tls12`: a
relaxation of something that is off cannot mean anything, and half-ignored configuration
is how postures rot.

### TLS 1.2 hardening conformance (audited against rustls 0.23 + this configuration)

Built as a **strict allowlist, tests enforcing exact sets** — a blocklist of known-bad
suites is never complete. "Structural" means rustls does not implement the hazard at all;
"pinned" means a test fails if a provider upgrade changes it.

| Area | Status |
|---|---|
| SSLv2/v3, TLS 1.0/1.1 | Structural + pinned: only 1.3 (and, opted-in, 1.2) are ever offered |
| Key exchange | Pinned allowlist: ECDHE only. No static RSA (ROBOT), no static DH, no anon, no export, no SRP/KRB5/PSK, **no FFDHE at all** (no Logjam/small-subgroup surface) |
| Curves | Pinned allowlist: exactly `x25519`, `secp256r1`, `secp384r1` classical (ML-KEM hybrids are 1.3-only `key_share` entries a 1.2 hello cannot negotiate). No binary/small curves; on-curve validation is rustls/aws-lc internal |
| Bulk ciphers | Pinned allowlist: exactly the six ECDHE AES-GCM / ChaCha20-Poly1305 suites. No CBC (Lucky13/POODLE class), no RC4, no 3DES (Sweet32), no NULL, no CCM_8 |
| Signatures | Structural (webpki/aws-lc): no MD5, no SHA-1 (RFC 9155), no DSA; RSA ≥ 2048; PSS + PKCS1-SHA256+ and ECDSA/Ed25519 only |
| Compression (CRIME) | Structural: not implemented |
| Heartbeat (Heartbleed) | Structural: not implemented |
| Renegotiation | Structural: not implemented at all — client-initiated renegotiation is rejected by construction, stronger than RFC 5746 |
| Truncated HMAC / NPN | Structural: not implemented |
| RFC 5077 session tickets (1.2) | **Off, pinned by test**: no ticket-key rotation infrastructure exists, and an unrotated ticket key silently destroys forward secrecy |
| Session cache lifetime | **Enforced here**: entries are timestamped and refused past 24 h (RFC 5246 §F.1.4) — capacity eviction alone would let a resumption secret stay redeemable for months |
| Extended Master Secret | **Enforced here**: required under the hardened posture; relaxed only by the explicit unsafe flag |
| GCM nonce + rekey ceiling | Structural: RFC 5288 counter nonces; rustls refuses further AES-GCM records after 2²⁴ per connection, forcing a fresh handshake before nonce-safety margins erode |
| Constant-time / record limits / zeroization | Delegated to rustls + aws-lc-rs by deliberate choice — the memory-safe-library route rather than hand-rolled record-layer code |
| Certificate validation | Structural (webpki): SAN-only matching, wildcard rules, basicConstraints/EKU, validity windows |
| **`TLS_FALLBACK_SCSV` (RFC 7507)** | **Deviation, accepted**: rustls does not implement server-side SCSV detection. Risk is bounded — the broker never initiates fallback, 1.2 exists only behind the opt-in, and modern clients no longer perform insecure fallback dances. Revisit if rustls grows support |
| **OCSP stapling / Must-Staple** | **Deviation, deferred**: no OCSP infrastructure; CRL-based revocation (T8) covers the operational need today. Tracked with the existing OCSP backlog item on #125 |
