# ADR 0017 — Durable attach waits for an authoritative session, never downgrades

- **Status:** Accepted
- **Date:** 2026-06-19
- **Deciders:** project maintainers
- **Related:** [ADR 0005](0005-session-affinity.md) (placement owns the session),
  [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (durable store + lease epochs), [ADR 0016](0016-swim-membership-stability.md)
  (membership stability — fixed the *replica set*; this ADR fixes the *attach path*),
  `docs/TEST-PLAN.md` (client-observable durable-failover gap)

## Context

After a takeover, a *persistent* client reconnecting to the **new owner** comes up
`session_present=false` and starts a fresh session — silently abandoning its real
durable session (subscriptions, queued messages). ADR 0016 phase 1 fixed the
membership half (the new owner's replica set is now the live survivors, and the store
recovers the session from a quorum in ~1s). The diagnosis then narrowed to the **attach
path**, and it is two distinct bugs:

1. **A transient error is downgraded to "no session."** The hub's attach does
   `self.store.ensure_session(&client).await.unwrap_or(false)`. During the ~1s window
   before the group's lease is reassigned to the new owner, `ensure_session` returns a
   *transient* `NotOwner` (the lease is in flux). `unwrap_or(false)` turns that into
   `session_present=false` — so a client reconnecting in that window is told it has no
   session and starts clean. **This is a durability violation:** a live, recoverable
   session is orphaned because of a momentary, self-healing condition. The same applies
   to `subscriptions`, `pending`, and `ack`, whose errors are swallowed by `if let Ok`.

2. **The recovery wait would freeze the whole hub.** The hub is a single sequential
   command loop (`dispatch(cmd).await` inline). Every durable read in attach already
   blocks the loop — no other client is served, no publish routed, for its duration
   (~1s on first-touch recovery today). So the naive fix — "retry `ensure_session` with
   backoff until the lease lands" — is unacceptable inline: it would freeze the hub for
   the entire retry budget, turning a takeover into a node-wide stall and a
   reconnect-flood into a denial-of-service vector.

The error type also can't currently distinguish transient from terminal: the cluster
store collapses `ReplError::{NotOwner, NoQuorum}` into `StorageError::Backend(String)`,
so the attach path can't classify without string-matching.

## Decision

### 1. A durable attach must obtain an *authoritative* result — or reject

The invariant: **the attach path never reports `session_present=false` because of a
transient or unknown error.** It either gets an authoritative `Ok(present)` from the
store, or — if the store cannot give one within a bounded deadline — it **rejects the
CONNECT** with a *Server unavailable* CONNACK (v3.1.1 return code `0x03`; v5 `0x88`) and
closes. The client retries; its durable session stays intact and is served on a later
attempt once the lease has reassigned (or quorum returns). A clean-start connect is
unaffected — it intentionally discards prior state and needs no recovery.

Rejecting is strictly safer than downgrading: downgrade *destroys* a recoverable
session (the client proceeds clean, its state orphaned); reject *preserves* it and
defers. "Server unavailable, try again" is exactly the truth during a lease handoff.

### 2. The recovery wait runs *off* the hub command loop

A new typed transient error, `StorageError::Unavailable`, is introduced (the cluster
store maps `ReplError::{NotOwner, NoQuorum}` to it; `Backend`/`NotFound` stay terminal).
The hub's `attach`, for a persistent session, **spawns** a recovery task holding a
cloned `Arc<dyn SessionStore>` handle. That task does the bounded retry —
`ensure_session`, `subscriptions`, and a `pending` probe (warming the offline-queue key
too, so the inline replay is reliable and never silently skipped) with backoff until an
authoritative result or the deadline — **without holding the hub loop**. It then sends
the result back as a new
`HubCommand::SessionRecovered`, whose handler runs the *fast, in-memory* registration
on the loop (reconcile subscriptions into routing, register `online`, fire any
takeover will, reply, resume in-flight QoS, replay the now-warm queue). The only work
removed from the loop is the *blocking durable reads*; all hub-state mutation stays
single-threaded on the loop, so there are no new data races.

Because the recovery task warms the per-key recovery (recover-once-per-epoch) before
registration, the on-loop reads in `SessionRecovered` (replay `pending`/`ack`) hit a
warm, lease-held log and are fast — this is *less* on-loop blocking than today, not
more.

### 3. Last-writer-wins across the off-loop window

Recovery is now asynchronous, so a second CONNECT for the same client id (takeover) can
arrive while the first is still recovering. The hub tracks `connecting[client] =
conn_id` when it spawns recovery; `SessionRecovered` proceeds only if that entry still
names its `conn_id`. A superseded recovery is dropped (its `reply` is dropped, so that
connection closes), and the newest connect wins — preserving today's takeover
semantics.

### 4. Bounded, configurable wait

The recovery deadline and backoff are constants for now
(`ATTACH_RECOVERY_TIMEOUT`, default 5s — comfortably above the observed ~1s lease
handoff, below a client's connect timeout; `ATTACH_RECOVERY_BACKOFF`, ~50ms→250ms).
They can become config later. The non-cluster `MemorySessionStore` never returns
`Unavailable`, so its attach resolves on the first attempt with no added latency.

## Consequences

- **Good:** closes the client-observable durable-failover gap — a persistent client
  reconnecting during/after a takeover resumes its session (`session_present=true`)
  once the lease lands, or is cleanly told to retry, but is **never** silently reset to
  a fresh session. No more swallowed transient errors. The hub loop is not frozen by the
  wait, so a recovering session (or a flood of them) does not stall other clients —
  closing a DoS vector. On-loop attach latency drops (recovery is pre-warmed off-loop).
- **Cost / limits:** a persistent connect during a lease handoff now *blocks up to
  `ATTACH_RECOVERY_TIMEOUT`* before either resuming or being told to retry — a deliberate
  trade (a short, bounded wait for correctness, vs. an instant but wrong "clean
  session"). Attach is now two-phase (spawn → `SessionRecovered`), adding a small amount
  of state (`connecting`) and one internal command. A genuinely unavailable store
  (no quorum) rejects connects until it recovers — correct, and visible as
  *Server unavailable* rather than silent data loss.
- **Risk:** this touches the attach/takeover critical path. It is developed test-first
  at the hub level with a fault-injecting store double (returns `Unavailable` N times
  then `Ok`), asserting: (a) a transient-then-ready store yields `Present(true)`, not a
  downgrade; (b) a permanently-unavailable store yields a *reject*, not `present=false`;
  (c) an immediate `Ok(false)` yields a fast `Present(false)`; (d) the hub loop keeps
  serving other commands while a recovery is in flight; (e) last-writer-wins on
  overlapping connects. Then the client-observable failover integration test (re-added)
  must pass deterministically.

## Alternatives considered

- **Retry inline in attach.** Rejected: freezes the single-threaded hub for the retry
  budget (§Context 2) — a node-wide stall and a DoS vector.
- **Return `present=false` but keep the durable session for later.** Rejected: the
  client has already been told it has no session; many clients will (re)subscribe and
  proceed as fresh, and a clean-session reconnect would wipe the orphaned state. Once
  you've answered "no session," the damage is done. The answer must be correct or
  deferred, not optimistic.
- **Shrink/skip the quorum on recovery to answer instantly.** Rejected (already, in ADR
  0016): reading below quorum can miss a committed entry — it trades a latency problem
  for a silent-correctness one.
- **Do recovery in the conn task via a separate readiness probe, then run the existing
  attach unchanged.** Viable, but it double-reads (`ensure_session` twice) and still
  needs attach to classify `Unavailable` to be race-safe; folding the wait into a single
  spawn-and-`SessionRecovered` mechanism is one code path, not two.
