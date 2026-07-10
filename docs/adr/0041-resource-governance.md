# ADR 0041 — Resource governance (admission caps, per-client quotas, bounded state)

- **Status:** Accepted
- **Date:** 2026-07-05
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0041-resource-governance.md](../delivery/0041-resource-governance.md) — plan, progress, and changelog
- **Related:** [ADR 0012](0012-flow-control.md) (Receive Maximum — the one per-client quota
  that exists; its inbound QoS 1 enforcement is finished here), [ADR 0011](0011-topic-aliases.md)
  (the bounded-alias-table precedent), [ADR 0017](0017-durable-attach-readiness.md) (the
  attach-storm mitigation — the recorded DoS framing this generalizes),
  [ADR 0009](0009-mqtt5-expiry.md) (expiry, the *time* bound complementing these *size*
  bounds), [ADR 0018](0018-on-disk-persistence.md) (the redb stores this puts under a
  disk watermark), [ADR 0020](0020-metrics-and-observability.md) (bounded-cardinality
  metrics; the pressure gauges land there), [ADR 0034](0034-foreign-client-interop-conformance.md)
  (reason-code conformance for the new rejections)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0041-resource-governance.md).

## Context

The broker bounds what a **single frame, packet, or session object** can cost: the read
buffer (1 MiB), the peer frame (16 MiB), the flow-control backlog (10 000, drop-oldest),
the offline queue (100 000, drop-oldest), the topic-alias table (16), the durable retained
mutation queue (1024), pre-CONNECT and auth-round timeouts. Each has a defined at-bound
behavior, and each is tested.

What is **not** bounded is everything an attacker — or an enthusiastic fleet — can have
*many* of:

- **Connections.** No cap on concurrent connections, none per source IP. Every accept
  loop spawns unconditionally; `SERVER_BUSY (0x89)`, `CONNECTION_RATE_EXCEEDED (0x9F)`,
  and `QUOTA_EXCEEDED (0x97)` exist in the codec and are never emitted.
- **Authentication attempts.** No rate limit, lockout, or backoff on failed CONNECTs.
  Argon2id makes each password check deliberately expensive — which, uncapped, hands an
  unauthenticated attacker a CPU lever, not just a brute-force one.
- **Per-client state.** Subscriptions per client are unbounded, and one SUBSCRIBE packet
  (bounded only by the 1 MiB frame) can carry hundreds of thousands of filters, each
  growing the routing table that every publish linearly scans. Publish rate is unbounded.
- **Global state.** The retained store accepts unbounded distinct topics from any client
  authorized to publish; total sessions (hence hub maps and durable rows) are unbounded.
  All of these *counts* are observable (ADR 0020) — none is *governed*.
- **Disk.** The redb stores grow with retained topics, sessions, and offline queues
  (100 000 messages *per session*); there is no size visibility and no watermark. On
  disk-full, the QoS ≥ 1 ack path fails closed (the publisher retries), but a cross-node
  offline enqueue failure is logged and dropped — inconsistent.
- **Operator control.** The one per-session anti-OOM lever that exists (`QueueLimits`)
  has no production configuration surface; the 1 MiB frame cap is a hard-coded
  placeholder that MQTT 5's Maximum Packet Size property was designed to negotiate.

The capability plan has carried this as an explicit bullet since day one ("rate limiting,
connection caps, max packet size, slow-loris protection; per-client and per-listener
quotas"). It is pre-release area ③ because at the first release these change from code
edits into operator-visible behavior contracts.

## Decision

**Every resource a client can multiply gets a cap; every cap gets a defined, tested
at-bound behavior and a metric. Caps are enforced at the cheapest possible point, prefer
pushback over punishment, and ship with generous defaults that an operator can tune with
env vars — in the same style as every bound the broker already has.**

### 1. Admission caps: refuse before spending

A global **max-connections** cap (default generous, `MQTTD_MAX_CONNECTIONS`) and a
**per-source-IP** cap (`MQTTD_MAX_CONNECTIONS_PER_IP`) are enforced **at accept, before
the TLS handshake**: an over-cap connection is closed immediately, counted, and logged.
Completing a TLS handshake (or an MQTT exchange) just to say `SERVER_BUSY` would spend
exactly the CPU the cap exists to protect — the polite CONNACK is reserved for caps that
require knowing who the client is. The per-IP table is itself bounded (an LRU of source
addresses), because an accounting structure that grows per-attacker would be the
vulnerability it guards against.

The existing pre-CONNECT timeout already covers slow-loris; it is unchanged.

### 2. Auth-failure pushback: failed attempts buy delay

Repeated authentication failures from a source IP put that IP in a decaying **penalty
box** (token bucket refilled over time): while penalized, new connections from it are
closed at accept, before any Argon2 work. This converts a brute-force or CPU-burn attempt
into a self-limiting trickle without any persistent lockout state to administer (and no
lockout lever an attacker can aim at a *victim's* credentials — the penalty keys on the
attacker's address, never on the username). Audited and counted; bounded like the per-IP
table.

### 3. Per-client quotas: the spec's own answer codes

- **Subscriptions per client** (`MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT`): a SUBSCRIBE filter
  that would exceed the cap is answered `0x97 Quota exceeded` (v5) / `0x80` failure
  (v3.1.1) in its SUBACK slot — per filter, so a partially-acceptable packet degrades
  instead of failing whole. The session keeps working.
- **Publish rate** (`MQTTD_MAX_PUBLISH_RATE`, messages/second, token bucket per
  connection): an over-rate publisher is **throttled by pausing the socket read** — TCP
  backpressure, the transport's native flow control. No packet is dropped, no session is
  killed, v3.1.1 and v5 behave identically, and a compliant-but-bursty client just slows
  down. Sustained abuse saturates its own connection, not the broker.
- **Inbound Receive Maximum, finished** (the ADR 0012 §3 deferral): QoS 1 overruns now
  count against the advertised quota exactly as QoS 2 already does — `DISCONNECT 0x93`.

### 4. Global state caps: growth is a grant

- **Retained topics** (`MQTTD_MAX_RETAINED_MESSAGES`, a global count): a retained publish
  that would create a **new** topic beyond the cap is refused — v5 gets
  `PUBACK/PUBREC 0x97 Quota exceeded`; v3.1.1 (which has no reason codes) delivers the
  message to live subscribers but does **not** retain it, loudly logged and counted.
  Overwriting or clearing an existing retained topic always works — the cap stops
  *growth*, never maintenance.
- **Total sessions** (`MQTTD_MAX_SESSIONS`): a CONNECT that would create a **new** session
  beyond the cap is refused with `0x97` (v5) / `0x03 Server unavailable` (v3.1.1);
  resuming an existing session always works — a full broker keeps serving its existing
  fleet and refuses only strangers.
- **Maximum Packet Size, negotiated** (`MQTTD_MAX_PACKET_SIZE`, default the current
  1 MiB): the transport read-buffer cap stops being a silent constant — the broker
  advertises it as the MQTT 5 Maximum Packet Size property in CONNACK, honors the
  *client's* advertised maximum on the outbound path (a message too large for the client
  is dropped for that subscriber, per spec, counted), and closes on inbound overrun as
  today. The placeholder becomes the contract.
- **Offline queue, operator-tunable**: `QueueLimits` gets its env vars
  (`MQTTD_MAX_QUEUED_MESSAGES`, and the drop policy), wiring the existing mechanism to
  the operator instead of only to tests.

### 5. Disk watermark: brownout, not blackout

Each redb store reports its file size as a gauge (ADR 0020). A soft **high-water mark**
(`MQTTD_STORE_MAX_BYTES`, off by default) puts the durable plane into **brownout** above
it: writes that *grow* state (new retained topics, new sessions, offline enqueues) are
refused with the same at-bound behaviors as §4, while acks, deletes, reads, expiry, and
resumes — everything that shrinks or maintains state — continue. A broker approaching
disk-full degrades to read-mostly instead of hitting the cliff where redb commits start
failing mid-write. The disk-full failure paths are made uniformly fail-closed while at
it (today a cross-node offline enqueue failure drops the message where the local ack
path correctly refuses to ack).

### 6. One config and observability story

Every cap: an `MQTTD_*` env var, a generous default (a cap nobody hits until they need
it), validation at startup (a nonsensical value is a startup error, not a silent
misconfiguration), a bounded-label metric for its rejections/throttles
(`admission_rejected_total{reason}`, `quota_rejections_total{kind}`, throttle counters,
store-size gauges — never per-client labels, ADR 0020 §3), and a line in the README's
configuration table. Limits are read at startup; hot-reloading them is deliberately out
of scope until there is operational evidence it is needed (the reload machinery exists,
ADR 0032 — adding limits to it later is mechanical).

## Consequences

- **Good:** a single client, address, or credential-guesser can no longer grow broker
  memory, disk, or CPU without bound; every rejection is spec-shaped (reason codes,
  TCP backpressure) rather than invented; operators get levers *and* gauges; the
  ADR 0012 deferral and the frame-cap placeholder are both paid off.
- **Cost:** accept-path bookkeeping (two bounded maps, a semaphore), a token bucket per
  connection, cap checks on the subscribe/retain/attach paths — all O(1) per operation;
  a dozen new env vars to document; store-size polling.
- **Risk:** a mis-set cap is a self-inflicted outage lever (the ADR 0040 risk, again).
  Mitigations: defaults generous enough to be invisible; caps that refuse *new* growth
  but never evict existing state (no cap disconnects a connected client or deletes
  data); startup validation; every rejection counted and attributable. Built test-first:
  each cap gets an at-bound test plus an under-bound test proving normal traffic is
  untouched.

## Alternatives considered

- **A general-purpose rate-limiting/quota framework (per-tenant classes, weighted
  buckets).** The broker has one operator and one trust domain per deployment today;
  tenant classes would be speculative structure. Single global + per-client caps cover
  the pre-release threat model; a tenancy layer can subsume them later. Rejected for now.
- **CONNACK `SERVER_BUSY` for over-cap connections.** Spec-polite, but requires
  completing the TLS handshake — the expensive step — for a connection the broker already
  decided not to serve; an amplification lever. Rejected in favor of close-at-accept
  (the reason codes are used where identity is already established: quotas, session cap).
- **Disconnect (`0x96 Message rate too high`) for over-rate publishers.** Lossy for
  bursty-but-compliant clients and creates reconnect storms (the ADR 0017 problem);
  read-pause throttling is invisible to a well-behaved client and self-limiting for an
  abusive one. The reason code remains available for a future hard ceiling. Rejected as
  the primary mechanism.
- **Byte-based bandwidth quotas.** Message-rate plus the (now negotiated) packet-size cap
  bounds bandwidth to `rate × size` with two understandable knobs; a third byte-rate knob
  adds config surface without a distinct threat. Deferred until evidence.
- **Username-keyed auth lockout.** Lets an attacker lock out a *victim* by failing their
  username on purpose — a denial-of-service lever aimed at legitimate users. The penalty
  box keys on source address only. Rejected.
- **Evict-oldest when a global cap is hit (sessions, retained).** Turns a cap into silent
  data loss for existing users in favor of strangers; refusing *new* growth is the
  fail-safe direction (matches the ADR 0040 principle that caps never destroy standing
  state). Rejected.
- **Hot-reloadable limits.** The ADR 0032 machinery could carry them, but limits differ
  from security policy: they change rarely and a restart is acceptable; keeping them
  startup-only avoids sweep semantics for capacity (what would "sweep" a lowered
  connection cap mean — mass disconnect?). Deferred with a recorded path back.
