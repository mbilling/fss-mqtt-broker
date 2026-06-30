# ADR 0023 — Gossip anti-replay: persisted monotonic sequence + sliding window

- **Status:** Accepted
- **Date:** 2026-06-22
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0023-gossip-anti-replay.md](../delivery/0023-gossip-anti-replay.md) — plan, progress, and changelog
- **Related:** [ADR 0003](0003-gossip-authentication.md) (the shared-key MAC + the bounded-replay
  argument this tightens), [ADR 0022](0022-signed-gossip.md) (the authenticated sender
  identity this binds the replay window to), [ADR 0018](0018-on-disk-persistence.md) (the
  data dir the sequence counter persists in)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0023-gossip-anti-replay.md).

## Context

[ADR 0003](0003-gossip-authentication.md) authenticates gossip datagrams but **accepts
replay**, arguing it is bounded and self-healing: SWIM's incarnation supersession means a
replayed `Alive`/`Suspect`/`Dead` at or below a member's current incarnation is ignored, and
a replayed `Dead` forces at most one refutation. That is a real bound, but a residual window
remains — an attacker can replay a captured datagram (a `Dead` claim, a `Ping`/`Join`) within
the incarnation/processing window. ADR 0003 deferred "full anti-replay (timestamp windows or
per-peer nonces)" as `0003-T7`.

The requirement for closing it: **strict** replay rejection that **does not depend on
cross-node clock synchronisation** (which skews and fails) and **does not lock a restarted
node out of the cluster** (the failure mode of a naive sequence counter).

## Decision

Add per-datagram sequence numbers with a sliding replay window, made clock-free by
persistence and DoS-free by binding to the authenticated sender. The new format is **v3 =
signed (ADR 0022) *and* sequenced** — anti-replay builds on per-node authentication, it does
not replace it.

### 1. Per-node monotonic sequence, persisted by block reservation

Every node stamps each outgoing datagram with a strictly increasing 64-bit sequence,
covered by the MAC. The counter is **globally monotonic across process restarts** without a
clock: it is persisted by *block reservation* — reserve `N` numbers on disk with a single
fsync, hand them out from memory, reserve the next block when the current one is exhausted;
on boot, resume **above** the last reserved block. A restart therefore only ever advances the
sequence (it may skip the unused tail of a block — harmless in a 2⁶⁴ space). Monotonicity
comes from persistence, never from time.

### 2. Per-sender sliding replay window at the receiver

Each receiver keeps, per sender, the highest sequence seen and a fixed-size window bitmap
(RFC 6479 style). A datagram is **rejected** if its sequence is at or below the window's low
edge, or its bit is already set; otherwise it is **accepted**, its bit set, and the window
slid forward on a new high. Gossip delivers a sparse subsequence of any sender's stream
(only datagrams addressed to this receiver), so the window slides over large gaps — which is
fine: a replayed old datagram falls below the window and is rejected; an exact duplicate
within the window is caught by the bitmap. State is a few bytes per peer, bounded by
membership.

### 3. Bind the window to the *authenticated* identity (requires ADR 0022)

A replay window keyed by an **unauthenticated** `from` is itself a denial of service: forge
`from=X, seq=huge` and X is locked out. So anti-replay is layered on
[ADR 0022](0022-signed-gossip.md) signing — wire format **v3 carries the sequence *and* the
node signature**, and the window is keyed by the certificate-verified Common Name. Without
X's private key an attacker cannot advance X's window.

### 4. Wire format v3, strict posture

`[VERSION=3][HMAC tag][seq u64][cert_len][cert][sig_len][sig][payload]`, the tag covering
everything after it. v1 (ADR 0003) and v2 (ADR 0022) remain understood as distinct
postures. Selected by `MQTTD_SWIM_REPLAY` = `require` (emit v3, reject anything without a
valid fresh sequence) / `off`. The posture is **strict**: a `require` node accepts only v3.
`require` implies signed `require` and needs a writable `MQTTD_DATA_DIR` for the persisted
counter; absent either it is a **startup error**, not a silent downgrade. (A transitional
`prefer` mode — emit v3 but still accept v2/v1 during a node-by-node rollout — existed
earlier but was **removed before any production release**: the mainline was never deployed,
so no zero-downtime upgrade path was ever needed.)

### 5. Failure modes, by construction

- **Clock skew:** impossible to affect correctness — there is no timestamp.
- **Sender restart:** the persisted counter only advances, so the sender is never locked
  out and its pre-restart datagrams (lower seq) are rejected as replays.
- **Receiver restart:** a fresh receiver has no window, so it accepts the first datagram per
  sender as a baseline. An attacker could inject one captured datagram per sender in that
  instant; its content is authentic cluster gossip and remains incarnation-bounded (ADR
  0003), and the next legitimate (higher-seq) datagram supersedes it. Bounded and benign.

## Consequences

- **Good:** strict replay rejection with **no clock-synchronisation dependency** and **no
  restart lockout** — the two failure modes that make naive anti-replay fragile. Composes
  with ADR 0022 to make a forged *or* replayed membership claim equally impossible.
- **Cost:** a persisted per-node counter (one fsync per reserved block — negligible at
  gossip volume) requiring a data dir; per-sender receiver state (a few bytes); +8 bytes
  per datagram plus the v2 signature overhead; the posture is a uniform deployment-time
  choice (no per-node rollout coexistence).
- **Risk:** correctness-critical security and distributed code. Built **test-first**: the
  sliding window and the block allocator are pure and exhaustively unit-tested (including
  restart-resumes-above-last-block and out-of-order/duplicate sequences); the wire format is
  pinned by known-answer tests; and an over-UDP integration test proves a captured datagram
  replayed to a peer is rejected while live traffic flows. Same bar as ADR 0003/0016/0022.

## Alternatives considered

- **Timestamp freshness window.** Stamp each datagram with wall-clock time and reject outside
  a skew window. Rejected: it depends on cross-node clock synchronisation — skew silently
  drops legitimate traffic or widens the replay hole, and a clock fault degrades security.
  The explicit requirement was a mechanism that does not fail on time sync.
- **In-memory sequence (no persistence).** Simplest, but a restarted node's counter resets
  and every receiver rejects its traffic until they too restart — a cluster-breaking lockout.
  Rejected.
- **Per-pair session handshake (negotiate a sequence space per peer, IKE/TLS style).**
  Strict and clock-free, but a connection handshake per peer is the wrong shape for
  connectionless, fan-out UDP gossip. Rejected as disproportionate.
- **Leave it bounded (ADR 0003 status quo).** Acceptable until now; this ADR is the
  hardening that closes the residual replay window when an operator wants it.
