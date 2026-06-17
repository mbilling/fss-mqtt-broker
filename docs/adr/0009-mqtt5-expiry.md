# ADR 0009 — MQTT 5.0 session & message expiry

- **Status:** Accepted (design); implementation phased (workstream G)
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Related:** [ADR 0001](0001-session-durability.md) (session lifecycle/storage),
  [ADR 0005](0005-session-affinity.md) (the owner serves a session),
  [ADR 0008](0008-mqtt-5-codec.md) (the v5 wire that carries these properties),
  [Cluster Durability Plan](../CLUSTER-DURABILITY-PLAN.md) workstream G

## Context

The v5 wire codec (ADR 0008) is complete; the broker now negotiates v5 but ignores
the v5 *semantics*. The first two are **session expiry** and **message expiry** — the
lifetimes of, respectively, a disconnected client's session state and an undelivered
queued message.

MQTT 3.1.1 has only `clean_session`: `1` discards session state at disconnect, `0`
keeps it **forever**. MQTT 5.0 splits this into two independent controls:

- **Clean Start** (the same CONNECT flag bit) — whether to *resume* an existing
  session at connect (`0`) or start fresh, discarding any prior one (`1`).
- **Session Expiry Interval** (CONNECT/DISCONNECT property `0x11`, seconds) — how long
  to *retain* the session after disconnect: `0` = discard at disconnect,
  `0xFFFFFFFF` = never expire, otherwise a deadline.

**Message Expiry Interval** (PUBLISH property `0x02`, seconds) — a queued message's
lifetime; if still undelivered when it elapses, drop it, and forward the remaining
interval on delivery.

The questions this fixes: how the two v5 controls map onto the broker's existing
`clean_session` lifecycle, where expiry is enforced (especially in a cluster), and how
a message carries its deadline through the store.

## Decision

### 1. Normalize both versions to (clean_start, session_expiry) at the connection edge

The hub speaks only `(clean_start: bool, session_expiry: u32)`; the connection layer
translates each protocol version into that pair:

- **v3.1.1:** `clean_start = clean_session`, and
  `session_expiry = if clean_session { 0 } else { 0xFFFFFFFF }` — exactly reproducing
  "discard now" vs "keep forever".
- **v5:** `clean_start` is the CONNECT clean-start bit; `session_expiry` is the
  `Session Expiry Interval` property (absent = `0`, per spec).

So the hub's lifecycle logic is single, version-agnostic, and the existing v3.1.1
behaviour falls out as the `{0, 0xFFFFFFFF}` special cases — no separate code path.

### 2. The hub owns session lifecycle; expiry is a periodic sweep on the owner

Session lifecycle already lives in the hub (`attach`/`detach`, the durable
`SessionStore`). Expiry extends it:

- **Attach.** `clean_start` discards any existing session first. The session's
  `session_expiry` is recorded, and any pending expiry deadline is cancelled (the
  client is back).
- **Detach.** `session_expiry == 0` discards immediately (the old `clean_session=1`
  path); `0xFFFFFFFF` keeps it indefinitely (the old `clean_session=0` path);
  otherwise the session is kept with a deadline `now + session_expiry`.
- **Sweep.** A periodic tick in the hub actor loop discards every session whose
  deadline has passed (drop subscriptions, in-flight, and `store.remove`).

Discarding is the same operation everywhere (a `discard_session` helper), so the
durable backend's `remove` (which quorum-replicates the deletion) and the in-memory
backend are both covered with one implementation.

**Cluster.** A persistent session is relocated to its placement owner (ADR 0005), so
the **owner's** hub holds it and runs its expiry — no cross-node coordination. *Carried
limitation:* the expiry deadline is in-memory on the owner. If the owner dies and a
replica takes over (workstream F), the session data survives (it is in the replicated
log) but the deadline is lost — the clock effectively restarts. Persisting the
disconnect time in the session's durable meta snapshot closes this and is a follow-up,
not phase 1.

### 3. Message expiry rides in the stored queue entry; the deadline is absolute

A queued message carries an **absolute expiry deadline** (not the original interval),
stored alongside it in the `SessionStore`. On enqueue, `deadline = now + interval`
(none if the property is absent). On replay/delivery: drop entries past their deadline,
and set the outbound `Message Expiry Interval` to the **remaining** seconds
(`deadline - now`), as the spec requires. An absolute deadline (rather than re-deriving
elapsed time) is what survives a broker restart or a takeover correctly.

This needs the stored message to gain an optional deadline; that is a storage-format
change (phase 2), kept separate from the session-expiry phase.

### 4. Typed property accessors, not raw `Vec` scans at every use

Per ADR 0008, the broker reads v5 properties through thin typed accessors on
`Properties` (e.g. `session_expiry_interval() -> Option<u32>`), added as each is needed
— keeping the generic wire model while giving the broker ergonomic, single-scan reads.

### 5. Phased implementation

1. **Session expiry** — the (clean_start, session_expiry) normalization, hub lifecycle
   + sweep GC, and the `session_expiry_interval` accessor. *(this phase)*
2. **Message expiry** — the stored deadline, drop-on-expiry at replay, remaining-interval
   on delivery.
3. **Durable expiry deadline** (the carried limitation in §2) — persist disconnect time
   so a takeover preserves the session-expiry clock.

## Consequences

- The v3.1.1 `clean_session` behaviour is now a degenerate case of the v5 model;
  existing tests must continue to pass unchanged (they pin the `{0, 0xFFFFFFFF}` cases).
- A new periodic tick enters the hub actor loop — cheap (a map scan), bounded by the
  number of disconnected-but-retained sessions on the node.
- Absolute deadlines make expiry correct across restarts and (once §2/§3 land) takeover,
  at the cost of trusting wall-clock skew between nodes — acceptable for second-grained
  expiry intervals.
