# ADR 0031 — Bind the session to the authenticated identity

- **Status:** Proposed
- **Date:** 2026-06-25
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0031-session-identity-binding.md](../delivery/0031-session-identity-binding.md) — plan, progress, and changelog
- **Related:** [ADR 0004](0004-identity-and-authentication.md) (the deny-by-default identity +
  ACL posture this extends to the session itself), [ADR 0005](0005-session-affinity.md) /
  [ADR 0007](0007-durable-store-integration.md) (placement + the durable session store, both
  keyed on the client id), [ADR 0009](0009-mqtt5-expiry.md) (session retention), [ADR
  0013](0013-enhanced-authentication.md) (v5 enhanced auth / re-auth, which can change the
  identity mid-session)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0031-session-identity-binding.md). **Proposed — up for
> review; the chosen mechanism (and its secure-by-default posture) is the main open
> question.**

## Context

An MQTT **session** is keyed on the **Client Identifier** alone — at every layer:

- the durable/offline session store keys all state by the id string
  ([`logged.rs`](../../crates/mqtt-storage/src/logged.rs): `meta_key = "m/{id}"` for
  subscriptions, the QoS-2 dedup window and the packet-id high-water; `queue_key = "q/{id}"`
  for the offline queue);
- the hub's in-memory per-session maps are all `HashMap<ClientId, _>`
  ([`hub.rs`](../../crates/mqttd/src/hub.rs): `online`, `session_expiry`, `inflight`, …);
- the cluster routes a session to its owner node by hashing the id —
  `placement.owner(client) = group_owner(hrw::stable_id(client) % NUM_GROUPS)`
  ([`placement.rs`](../../crates/mqtt-cluster/src/placement.rs)).

Authentication is **separate**: a connection carries an authenticated `principal` (username,
or the mTLS certificate CN — ADR 0004), held as `principal.subject`. But that identity is
**not part of the session key** and is **not consulted** when a session is resumed or taken
over. The [`Authorizer`](../../crates/mqtt-auth/src/lib.rs) trait gates *topics*
(`authorize_publish` / `authorize_subscribe`) — there is **no `authorize_connect`**, and no
binding of "who may use client id `X`".

The consequence is a **session-takeover / fixation gap across identities**. MQTT specifies
that a second CONNECT with an existing Client Identifier **takes over** the session
([`hub.rs`](../../crates/mqttd/src/hub.rs) "session takeover: replacing existing
connection"). Today that takeover succeeds **regardless of who authenticated**: principal *A*
establishes a persistent session with id `X`; principal *B* — a *different* authenticated
user — connects with id `X` and **seizes** it (disconnecting *A*, inheriting *A*'s
subscriptions and queued messages, or fixing a session *A* will later resume). Deny-by-default
ACLs gate the *topics B can use*, but nothing binds the *session* to its owner. In a
multi-tenant deployment two tenants that pick the same id collide for the same reason.

This is technically spec-conformant (the Client Identifier *is* the session identity), but it
is a poor fit for a **security-first** broker whose whole posture is deny-by-default and
least-privilege: the session — a security-relevant resource holding queued data and
subscriptions — is the one thing not tied to an authenticated principal.

## Decision

**Bind a session to the authenticated identity that created it: a persistent session may be
resumed or taken over only by a connection whose authenticated `principal` matches the
session's owning identity.** Secure-by-default, no configuration required.

Mechanism (the **takeover/resume guard**, option C below):

1. On the first CONNECT that creates a persistent session, record the owning identity
   (`principal.subject`) in the session metadata (durable, so it survives across nodes and
   restarts — it travels with `SessionMeta`).
2. On a later CONNECT for an existing id, compare the new connection's `principal.subject` to
   the stored owner. **Match → resume/takeover as today. Mismatch → reject** the CONNECT
   (CONNACK `0x87` Not authorized / 3.1.1 code 5) — the second principal cannot seize the
   first's session; it picks a different id or is denied.
3. The **anonymous** principal is treated as a single shared identity: when
   `allow_anonymous` is on, anonymous clients share a session namespace (already an
   explicitly-insecure mode, ADR 0004 — no isolation is promised there). A real boundary
   runs authenticated.

A complementary, **opt-in** policy lever (option B) may be layered on top: an
`authorize_connect(identity, client_id)` hook on the `Authorizer` so an ACL can constrain
*which* ids an identity may claim at all (e.g. a per-identity prefix `tenantA/*`), for
deployments that want id-namespacing as policy rather than only first-claim binding. The
guard (1–3) is the secure-by-default core; the connect ACL is the configurable refinement.

### Open questions (to refine before ratifying)

- **Identity rotation.** A legitimate identity change (cert CN change / re-issue, a username
  change) would make an existing session unrecoverable under a strict match. Do we key on the
  CN, a stable subject claim, or allow an operator-defined equivalence? (Interacts with ADR
  0013 v5 re-auth, which can change identity mid-session.)
- **Key vs guard.** The guard (C) defends the hijack with minimal blast radius. **Namespacing
  the key** itself — making the effective session key `(identity, client_id)` everywhere
  (store keys, hub maps, the placement HRW hash) — is *stronger* (two identities with the
  same id are simply different sessions, never even a conflict) but reshapes the same
  cross-cutting surface ADR 0021/0030 touched. Which do we want as the end state?
- **Failure mode on mismatch.** Reject the CONNECT, or silently start a *fresh* clean session
  for the new identity (never touching the existing one)? Reject is louder and auditable;
  fresh-session is friendlier to clients that reuse a default id.

## Consequences

- **Good:** a session becomes a least-privilege resource tied to its authenticated owner; the
  cross-identity takeover/fixation gap closes; multi-tenant id collisions become a rejection,
  not a silent seizure; and it extends the ADR 0004 deny-by-default posture to the session
  itself. Secure-by-default (the guard needs no config); auditable (a mismatch is a logged,
  reason-coded rejection).
- **Cost:** the session metadata grows an owning-identity field (durable codec + cluster
  carry, the ADR 0030 pattern); the attach path compares identities; identity-rotation
  ergonomics need an answer. The optional connect ACL adds an `Authorizer` method and policy
  surface.
- **Risk:** medium and security-critical — it changes who may resume a session, so it is
  built test-first with adversarial tests (a different principal must *never* resume or take
  over another's session; the same principal always can; anonymous behaves as documented),
  the same bar as ADR 0003/0004/0023/0025.

## Alternatives considered

- **C — takeover/resume guard (the proposed decision).** Store the owning identity, compare on
  resume/takeover. Minimal blast radius, secure-by-default, defends the actual hijack.
- **A — namespace the session key by identity** (`(identity, client_id)` everywhere). The
  strongest isolation — collisions cannot even arise — but reshapes the store keys, hub maps,
  and the placement HRW hash (a cross-cutting change). Recorded as the candidate *end state*
  if first-claim binding proves insufficient.
- **B — connect ACL only** (`authorize_connect(identity, client_id)`, no binding). Flexible
  operator policy (per-identity id patterns) but **not** secure-by-default: an operator who
  configures nothing still has the gap. Best as a *complement* to C, not a replacement.
- **Do nothing (spec-conformant today).** The Client Identifier is the session identity per
  the MQTT spec, and ACLs gate topics. Rejected for a security-first broker: the session
  itself should be bound to its authenticated owner, not seizable by any authenticated peer
  that guesses or reuses the id.
