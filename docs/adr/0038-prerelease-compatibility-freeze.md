# ADR 0038 — Pre-release compatibility freeze (versioned wire, stamped schemas, final codecs)

- **Status:** Proposed
- **Date:** 2026-07-04
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0038-prerelease-compatibility-freeze.md](../delivery/0038-prerelease-compatibility-freeze.md) — plan, progress, and changelog
- **Related:** [ADR 0002](0002-transport-security.md) (the mTLS bus these frames ride),
  [ADR 0022](0022-signed-gossip.md) (the gossip plane, already strictly versioned),
  [ADR 0037](0037-durable-retained-messages.md) (the retained codecs this finalizes),
  [ADR 0018](0018-on-disk-persistence.md) (the redb stores this stamps)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0038-prerelease-compatibility-freeze.md).

## Context

There are **no production deployments yet**. That has let every recent delivery change
the peer wire protocol and on-disk formats freely and in place — `PeerMessage` variants
gained fields four times during ADR 0037 alone — which is exactly right *now* and
becomes impossible the day the first release ships. Three artifacts freeze implicitly at
that moment, and none of them currently carries the machinery to evolve afterwards:

1. **The peer bus has no version negotiation.** Frames are positionally-encoded bincode;
   the link handshake (`Hello`) carries only a node id. Two builds that disagree about
   any frame shape produce decode errors and link teardown — there is no way for a
   rolling upgrade to even *detect* the disagreement, let alone bridge it. (The gossip
   datagram plane already has strict version postures; the TCP bus is the gap.)
2. **The persistent stores have no schema markers.** `sessions.redb`, `replicas.redb`,
   `lease.redb`, and `retained.redb` open whatever they find. A future build reading an
   older layout would misinterpret bytes silently instead of refusing loudly; writing a
   migration without a version to dispatch on is guesswork.
3. **The retained codecs are one field short of final.** The durable retained record and
   the retained wire frames carry `(payload, qos, token)` but not the MQTT 5 application
   properties — the documented ADR 0037 P4/P5 caveat: retained replay on a *remote* node
   drops Content-Type, User Properties, and friends. Changing these codecs is free
   today; after release it is a migration plus a mixed-cluster story.

The window to fix all three with plain in-place edits closes at the first release.

## Decision

**Before the first release: freeze the bootstrap frames and version everything behind
them; stamp every persistent store with a schema version that fails closed; and finish
the retained codecs to full MQTT 5 fidelity while changing them is still free.**

### 1. A frozen handshake, a versioned link

`Hello` gains the peer-bus protocol range this build speaks: `proto_min ..= proto_max`
(both `1` today). On link establishment each side checks for overlap; **disjoint ranges
reject the link, loudly** (fail closed — a node that cannot agree on a protocol must not
half-join the mesh). The negotiated version of a link is `min(proto_max_a, proto_max_b)`.

From this ADR onward, the encodings of **`Hello` and `ProxyHello` are frozen forever**:
they are the bootstrap frames a build of any future version must be able to exchange to
discover disagreement. Every other frame may evolve — behind a `proto_max` bump, with
the sender constrained to the link's negotiated version. (Today all builds speak exactly
version 1, so no per-link version threading exists yet; the *handshake* is what must be
in the field before divergence is possible.)

### 2. Stamped schemas, fail-closed opens

Every redb store opens through a shared schema gate: a `schema_meta` table holding the
layout version. An absent marker (fresh file) is stamped with the current version; a
matching marker proceeds; **any other version refuses to open with an explicit error**
naming found-vs-expected. Pre-1.0 there are no migrations — the documented recovery for
a version bump is wipe-and-rejoin (the durable plane rebuilds a node from its peers) —
but the marker is what makes *post*-1.0 migrations writable at all.

### 3. Final retained codecs: full MQTT 5 fidelity

The retained pipeline carries application properties end to end: the durable record
codec (`r/<topic>` keyspace), the `RetainedCommit` / `RetainedUpdate` /
`RetainedSnapshot` frames, and the persistent retained store. A retained message
replayed from any node's cache is byte-equivalent in properties to one replayed where
it was published — closing the ADR 0037 caveat before the formats freeze.

### 4. Named wire shapes and a frame inventory

Multi-field wire entries become named serde structs (positional tuples make field
additions error-prone to review), and the delivery records the frame inventory:
**frozen** (`Hello`, `ProxyHello`) vs **versioned** (everything else).

## Consequences

- **Good:** rolling upgrades become *possible* to build later (the handshake detects
  divergence today, bridges it tomorrow); schema changes get a dispatch point and a loud
  failure instead of silent corruption; retained replay is spec-faithful everywhere; the
  last known pre-release codec debt is paid while it costs nothing.
- **Cost:** `Hello`/`ProxyHello` can never change shape again — deliberate; the schema
  gate adds one tiny table per store; the retained frames grow by the (usually empty)
  properties encoding.
- **Trade-off accepted:** pre-1.0 schema bumps mean wipe-and-rejoin rather than
  migration — the durable plane's peer recovery makes that safe, and writing migrations
  for unreleased layouts is wasted work.

## Alternatives considered

- **Do nothing until 1.0.** The freeze then happens implicitly and unmarked — the first
  post-release change discovers there is no version to negotiate against and no marker
  to migrate from. Rejected: this ADR is a few days of work; the alternative is a
  protocol fork.
- **Full feature-bit negotiation now.** Capability flags per feature, sender-side
  downgrade paths. Rejected as YAGNI: with exactly one version in existence, a range
  handshake is the entire requirement; feature bits can ride a future version bump if
  ever needed.
- **Per-frame versioning.** A version byte on every frame. Rejected: one negotiated
  version per link is simpler, and mixed-version *frames* on one link is not a state we
  ever want to reason about.
