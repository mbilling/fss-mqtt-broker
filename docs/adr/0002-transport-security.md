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
  that work starts.
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
