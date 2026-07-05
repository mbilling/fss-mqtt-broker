# ADR 0040 — Revocation reaches live state (eviction on reload)

- **Status:** Proposed
- **Date:** 2026-07-05
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0040-revocation-reaches-live-state.md](../delivery/0040-revocation-reaches-live-state.md) — plan, progress, and changelog
- **Related:** [ADR 0032](0032-hot-reloadable-security-policy.md) (the reload mechanism this
  extends; its "next operation" semantics are the gap), [ADR 0033](0033-config-file-watch-reload.md)
  (the auto-reload trigger, unchanged), [ADR 0002](0002-transport-security.md) (client-listener
  CRL — T8 — and the deferred peer-bus TLS reload this finishes), [ADR 0022](0022-signed-gossip.md)
  (gossip-plane revocation, already per-datagram), [ADR 0004](0004-identity-and-authentication.md)
  (the principal model), [ADR 0031](0031-session-identity-binding.md) (session-owner binding at
  resume), [ADR 0005](0005-session-affinity.md) (proxied sessions riding peer links),
  [ADR 0020](0020-metrics-and-observability.md) (the audit/metrics surface)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0040-revocation-reaches-live-state.md).

## Context

Every revocation mechanism the broker has is enforced at **admission time** — the next
CONNECT, the next TLS handshake, the next client-initiated operation, the next gossip
datagram. Nothing revokes **already-live** state. Concretely, after a successful policy
reload (ADR 0032/0033):

- A client whose certificate is now on the CRL keeps its **open TLS session**
  indefinitely — the mTLS verifier runs only at handshake, and the existing test
  (`reloading_a_crl_revokes_a_client_in_place`) deliberately drops the connection *before*
  the reload and asserts rejection only on a fresh handshake.
- A user **deleted from the password file** keeps publishing and subscribing on their
  open connection — `authenticate()` runs once at CONNECT, and MQTT 5 re-auth is
  client-initiated only (ADR 0013), so the server can never force a re-check.
- A **tightened ACL** denies the client's *next self-initiated operation* (the ADR 0032
  promise) — but existing subscriptions are **grandfathered**: the hub's fan-out performs
  no authorization, so a subscriber whose read access was revoked keeps receiving matching
  messages until it happens to issue a new SUBSCRIBE.
- An **established peer link** of a node whose cluster certificate is revoked keeps
  carrying data-plane and durable-plane frames. The gossip plane rejects the node's
  datagrams per-datagram (ADR 0022 T7) — but the peer-bus TCP link validated its cert
  once, at handshake, and the peer acceptor/connector are built once at startup (peer-bus
  TLS reload was explicitly deferred out of ADR 0032).
- A durable session whose owner was removed is unreachable (the owner can no longer
  authenticate; a different subject is blocked by ADR 0031) but its **state persists**
  until session expiry — inert, not revoked.

For a security-first broker this is the difference between "revocation" and "revocation,
eventually, if the target cooperates". A revoked certificate or deleted user is precisely
the case where the target does *not* cooperate: a compromised credential holder keeps
their session alive forever by simply never reconnecting. The window this ADR closes is
the one an attacker actually uses.

The pieces the fix needs already exist: policy state is hot-swappable behind `watch`
handles with a single reload point (ADR 0032), reloads are validated-before-swap and
audited, the hub can force-close a connection (the session-takeover path), and both
planes have CRLs (client CRL — ADR 0002 T8; cluster CRL — ADR 0022 T7).

## Decision

**A successful policy reload triggers a sweep of live state against the new policy.
Identity-level revocation terminates the session; permission-level tightening removes the
grant. Nothing waits for the client's next move.**

### 1. The sweep: reload-triggered, not delivery-time

Enforcement stays out of the per-message hot path. On every **successful** reload (SIGHUP
or file-watch — both call the same routine), after the new policy is published, the hub
sweeps its live state **once**: every online connection's admission facts and every stored
subscription are re-evaluated against the new policy. A failed reload (validate-before-swap,
ADR 0032) sweeps nothing — the running policy did not change.

The sweep re-evaluates **server-side revocable facts** only: certificate serials against
the new CRL, password-user existence against the new credential store, principals and
filters against the new ACL. The broker does **not** retain client credentials (passwords,
bearer tokens) to replay through the new authenticator — retention of replayable secrets
is a worse property than the gap it would close. JWT-authenticated principals are bounded
by their token's own expiry plus the ACL sweep, and MQTT 5 re-auth remains available to
clients (ADR 0013).

### 2. Identity revoked → session terminated

Each connection records at admission: the principal, how it authenticated (anonymous,
password, JWT, mTLS subject), and — when a client certificate was presented — the leaf's
serial. The sweep **disconnects** (MQTT 5: DISCONNECT reason `0x87` Not authorized, then
close; MQTT 3.1.1: close, the protocol has no server DISCONNECT) any online connection
whose:

- presented client-certificate serial is on the new CRL,
- password-authenticated user no longer exists in the new credential store, or
- principal is denied by the new ACL's connect rule.

Termination is identity-level and deliberate: the client's *right to a session* was
revoked, so the session ends. Its durable state remains (subject to §4) — a reconnect
re-runs the full admission gauntlet against the new policy and fails there.

### 3. Permission tightened → grant removed

A subscription whose filter the new ACL denies for its principal loses its **grant**: the
hub removes it from the routing table and from the session's durable subscription set, for
online *and* offline sessions, and stops queued-message replay for it. The client is *not*
disconnected for a permission change — its next SUBSCRIBE re-attempt is denied at the
ADR 0032 check like any new operation. This keeps the line crisp: **who you are** being
revoked ends the session; **what you may read** being revoked ends the flow.

(Publish tightening already works: every inbound PUBLISH is re-checked per ADR 0032. The
grant sweep closes the subscriber half.)

### 4. The peer bus: reloadable TLS, revoked links torn down

The deferred half of ADR 0032 lands here. The peer-bus acceptor/connector move behind the
same reload mechanism as the client listener (rebuilt on reload from the cluster CA, node
cert/key, and cluster CRL), so a rotated cluster cert is served on the next peer handshake.
Established peer links record the remote leaf's serial at handshake; the sweep **tears
down** any link whose remote certificate the new cluster CRL revokes. The mesh reacts as
to any link loss: SWIM (already rejecting the revoked node's datagrams per ADR 0022 T7)
marks it dead, placement and leases move, and the revoked node cannot re-handshake.
Proxied sessions (ADR 0005) riding a torn link drop with it and re-admit — against the
new policy — wherever they reconnect.

### 5. Durable state of a removed identity stays inert (recorded, not purged)

The sweep does not delete durable session state whose owner was removed. The state is
unreachable — resume requires authenticating as the owner (fails against the new store)
and a different subject is refused by ADR 0031 — and session expiry reaps it on schedule.
Purging on user-removal would make an operator typo in a credential file destroy
irreplaceable queued data; inert-until-expiry is the fail-safe default. The admission-side
block is pinned by test as part of this bundle.

### 6. Every eviction is audited and counted

Each sweep action emits an audit event (`security.evict`, with the client/node, the reason
— `cert-revoked`, `user-removed`, `connect-denied`, `grant-revoked`, `peer-revoked` — and
the triggering reload) and increments `revocation_evictions_total{kind}` (ADR 0020). The
reload audit event gains the sweep summary. An operator who publishes a CRL can *see* it
reach live state.

## Consequences

- **Good:** revocation means now, on every plane — client sessions, subscriptions, peer
  links — with one mental model (sweep on reload) and no per-message cost; the ADR 0032
  deferred item (peer-bus TLS reload) is paid; the compromised-credential window closes.
- **Cost:** connections and peer links carry a little admission metadata (principal
  source, leaf serial); the hub gains a sweep pass (O(connections + subscriptions), on
  reload only); the authenticator trait grows a user-existence probe for the password
  store.
- **Risk:** an eviction sweep is a self-inflicted-outage lever — a bad-but-parseable ACL
  push could disconnect a fleet. Mitigations: validate-before-swap already rejects
  malformed files; the sweep only acts on *differences* (an unchanged policy evicts no
  one); every eviction is audited with its reason; and grant-removal (not disconnect) for
  permission changes bounds the blast radius of ACL edits to exactly the revoked flows.
  Built test-first like ADR 0032: each eviction class gets a live-state test proving both
  the eviction and that untouched sessions keep flowing undisturbed.

## Alternatives considered

- **Delivery-time authorization (re-check ACL on every fan-out).** Closes only the
  subscription gap — not revoked certs, removed users, or peer links — at a per-message
  hot-path cost that grows with fan-out. The sweep is O(state) once per reload instead of
  O(messages) forever. Rejected.
- **Periodic re-validation (background rescan every N seconds).** Adds a revocation
  latency window and a permanent background cost to solve a problem that only exists at
  policy-change time; the reload is the exact moment the answer can change. Rejected —
  the sweep hooks the event itself.
- **Retain client credentials and replay them through the new authenticator.** Would
  catch rotated JWT keys and password *changes* (not just removals), but makes the broker
  a warehouse of replayable secrets — a strictly worse security posture than the residual
  gap (JWT lifetime is bounded by the token's own `exp`; password changes still gate every
  new session). Rejected.
- **Server-forced MQTT 5 re-authentication.** The spec makes re-auth client-initiated
  (ADR 0013); a server AUTH out of the blue is a protocol violation clients will not
  handle. Rejected on conformance (ADR 0034).
- **Purge durable sessions on user removal.** Symmetric, but turns a credential-file edit
  into irreversible data destruction with no undo; inert-until-expiry plus the ADR 0031
  resume block yields the same access outcome without the foot-gun. Rejected (an explicit
  operator purge tool can come later if needed).
- **Disconnect on any ACL tightening (no grant-removal tier).** Simpler, but punishes a
  client holding ten grants for losing one, and turns routine policy edits into mass
  disconnects. The two-tier rule (identity → terminate, permission → stop the flow)
  matches what each revocation actually revoked. Rejected.
