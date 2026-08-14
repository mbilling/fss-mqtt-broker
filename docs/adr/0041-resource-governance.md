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
resumes — everything that shrinks or maintains state — continue. Three growth writes are
deliberately exempt, each protecting an honesty property worth more than its bytes: the
inbound `QoS` 2 dedup record (written before the refusal is decided, #165's ordering),
SUBSCRIBE persistence, and the detach spill of already-accepted messages — so session
metadata still grows slowly under a sustained brownout (the enumeration in
`store_watch.rs`'s module doc is the authoritative list). A broker approaching
disk-full degrades to read-mostly instead of hitting the cliff where redb commits start
failing mid-write. The disk-full failure paths are made uniformly fail-closed while at
it (today a cross-node offline enqueue failure drops the message where the local ack
path correctly refuses to ack).

**As delivered — the cadence is a knob, and it bounds both overshoot and recovery
(0041-T14, issue #243).** The watcher interval is no longer the hard-coded 10 s this section
was first delivered with: `MQTTD_WATERMARK_POLL` / `[limits] watermark_poll_secs` (default
10, range 1-300, refused in `validate()`) sets it for BOTH axes, and each watcher
self-accelerates to `poll / 10` (floor 1 s, never longer than the configured value) while
its last sample sat within 10% of the mark. So the overshoot the mechanism concedes is
`interval x growth rate` plus the write in flight — a number the docs now print — and the
same interval bounds RECOVERY: a browned-out node is by definition inside the band, so this
is how long the T11 publish refusal outlives the pressure that caused it. The enumeration
of what "refused" means is unchanged, but it gains a FOURTH non-airtight edge, and unlike
the three above it can be full message payloads: a browned-out node keeps applying its
peers' already-committed appends into `replicas.redb` for groups it merely FOLLOWS, because
the refusal is decided at the group's session owner and `brownout` is consulted nowhere in
`mqtt-cluster`. Refusing there would thin the group's replica count rather than enforce a
watermark, which is `min_replicas`' business — so it stays, documented (`store_watch.rs`
remains the authoritative list). Also delivered: an edge-triggered WARN naming any single
store holding more than 70% of the aggregate mark (clearing below 60%), which is the
visibility half of T9.

**As delivered — the publisher's ack under brownout (0041-T11, issue #238).** The "acks …
continue" above meant ack *processing* — subscriber PUBACKs, deletes, truncation, the
things that **shrink** state. It was implemented as something else: a `QoS` ≥ 1 publisher
still received a PUBACK for a growth write brownout had refused, so for an offline
persistent subscriber the message existed nowhere and nobody was ever told. That is a
standing violation of the product's headline claim, and this section's own last sentence
already commits to the opposite reflex.

As delivered: **a `QoS` ≥ 1 publish that needs a durable append it cannot get is refused,
not acked** — for online and offline persistent subscribers alike (delivering live with no
durable record would promise a redelivery the store cannot honour, the rule the store-error
path already followed). v5 is answered `PUBACK`/`PUBREC 0x97 Quota exceeded`; v3.1.1, which
has no reason byte, gets no ack and a connection close, exactly as the store-error path
does today — and that close is a *broker-initiated* close, so a v3.1.1 publisher's Will
fires ([MQTT-3.14.4-3]). Counted as `quota_rejections_total{reason="brownout-publish"}` —
a refusal the publisher is *told about* is not a loss, so `publish_dropped{reason="brownout"}`
covers the losses nobody is told about: the `QoS` 0 offline enqueue (nothing was owed) and
the lost durable copy of an *ungated* publish — a Will, a settle-window back-fill — whose
**live delivery still happens**: suppressing a live send is only justified when a publisher
is being told, and a Will has no publisher left to tell.

The refusal is decided **before any effect**: nothing is stored, nothing is sent live to
anyone, no retained value is overwritten, no peer forward leaves the node, and a shared
subscription's round-robin turn is not consumed. And it **travels** (0041-T12): when the
refusing node is a peer — the *common* case for an offline persistent subscriber, whose
quorum-replicated session usually lives on another node — the refusal crosses the peer bus
as a verdict (peer proto 7) and the origin answers its publisher with the same per-version
answer above. On a link still negotiating an older proto (the rolling-upgrade skew window,
never a permanent state) the verdict degrades to a withheld ack and a connection close, so
a v5 publisher can briefly see the v3.1.1 answer mid-roll. A cross-node *shared* delivery
refused by its selected member re-selects within the group before the publisher is
refused; an unreadable or failed verdict withholds the ack — never a fabricated answer.

The honest cost: MQTT's acknowledgement is per-PUBLISH, not per-subscriber, so a publish
matching **any** persistent subscriber at `QoS` ≥ 1 is refused as a whole. Brownout is
therefore a partial *publish outage* for those topics rather than a silent lie — the trade
this ADR prefers, and the reason the watermark is off by default. It is bounded by
construction: a v5 reason ≥ 0x80 releases the send-quota slot and makes the packet id
reusable ([MQTT-3.3.4-9], §4.9), so each refusal terminates in O(1) with an application-
level delivery error and no accumulated state on either side; a v3.1.1 close costs the
broker no per-attempt state and is paced by the client's own backoff. And it is
self-healing: brownout refuses only *growth*, so consumption, deletes and expiry drain the
store until the edge lifts.

Unchanged: the retained-growth refusal keeps §4/T4's answer (v5 `0x97`; v3.1.1 delivered
live, not retained) — and brownout gates retained *growth* through the same check, so a
v3.1.1 retained publish under brownout that owes no durable enqueue is likewise answered
with a plain PUBACK, delivered live, and not retained. The offline-queue overflow policies
also keep their ack-and-drop: the **default** `drop-oldest` truncates the oldest
*already-acked* entries out of a full session queue (`publish_dropped{reason="queue-overflow"}`)
and the opt-in `reject-newest` acks and sheds the newest — a cap's shed is the stated
policy rather than a failure to honour one.

### 6. One config and observability story

Every cap: an `MQTTD_*` env var, a generous default (a cap nobody hits until they need
it), validation at startup (a nonsensical value is a startup error, not a silent
misconfiguration), a bounded-label metric for its rejections/throttles
(`admission_rejected_total{reason}`, `quota_rejections_total{reason}` — the Prometheus
label is `reason`; `kind` is only the OTel attribute name — throttle counters,
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

## Amendment (2026-08-04): byte-based bounds and a memory watermark

The 2026-08-04 pre-release sizing review (docs/SIZING.md, and the operational-limits
section it added to docs/COMPARISON.md) produced the evidence this ADR's deferrals
asked for. Three additions are **accepted**; implementation is tracked as 0041-T6..T8
in the delivery doc and ships as its own reviewed feature work.

1. **Per-session byte bound on the offline queue** (`max_queued_bytes`, env
   `MQTTD_MAX_QUEUED_BYTES`; unset = unbounded, both bounds enforced when both set —
   first reached wins, mirroring mosquitto's pairing). The count cap alone bounds
   memory/disk only when multiplied by `max_packet_size`: 100 000 messages × 1 MiB
   default packets is ~100 GiB *per session*. A count is not a budget; a byte bound
   is. Same overflow semantics as the count bound (`queue_overflow`).

2. **Bridge-spool byte bound.** The mqtt-bridge spool bounds messages (default
   10 000) but not bytes; the same count-is-not-a-budget argument applies to a
   boundary link buffering large payloads. A `max_bytes` joins `max_messages`,
   drop-oldest, counted.

3. **Process-memory watermark → brownout** (`memory_max_bytes`, env
   `MQTTD_MEMORY_MAX_BYTES`; unset = off). The RSS analogue of the T5 disk
   watermark, reusing its shape: a poller samples process RSS, an edge-triggered
   brownout refuses growth (new sessions, new retained topics, offline enqueues —
   all already-built refusal paths) while acks, reads, deletes, expiry, and resumes
   continue, and dropping below the mark restores growth. The 0041-T11 correction
   above is axis-agnostic — the brownout flag is the OR across axes — so the memory
   watermark refuses a `QoS` ≥ 1 publisher's ack identically to the disk one, and
   "acks continue" here likewise means subscriber/maintenance acks. This is
   deliberately NOT mosquitto's allocation-failure model (deny malloc at a heap cap)
   nor EMQX's `force_shutdown` (kill the connection process over a per-connection heap/mailbox
   bound): both destroy standing state or sessions; brownout refuses new growth,
   consistent with this ADR's founding principle. Operators who prefer a hard
   ceiling keep the container limit — the watermark's job is to make the limit
   unnecessary in the common case. (The issue-#243 amendment below puts a number on
   "watermark, not a ceiling": overshoot ≤ the watcher interval × the allocation rate, so
   the container limit must sit at least that far above the mark.)

Reaffirmed as deferred: per-tenant/weighted quota classes, byte-*rate* bandwidth
quotas (`rate × size` still bounds bandwidth; the additions above bound *state*, a
distinct axis), and hot-reloadable limits (unchanged reasoning).

## Amendment (2026-08-14): the watermarks' three conceded gaps, decided (issue #243)

A review panel attacked resource governance on three fronts: the memory watermark is not a
ceiling, the disk watermark is aggregate-only, and both watchers polled every 10 s. One ends
in code, one splits, and one is refused on merit. Recorded here because two of the three are
now *stated* limitations rather than tracked work, and a limitation with no argument behind
it decays into an excuse.

1. **The poll interval — fixed in code** (0041-T14). `MQTTD_WATERMARK_POLL`, default 10 s,
   range 1-300 (`validate()` refuses outside it, per §6), one knob for both axes because a
   `/proc/self/status` read and four `stat` calls cost the same nothing, plus near-mark
   self-acceleration to `poll / 10` (floor 1 s). The overshoot bound
   (`interval x growth rate + the write in flight`) and the recovery tail are now numbers in
   SIZING and the README rather than the adjective "soft". Deliberately NOT done: charging
   the mark at append time, which would bound overshoot by ONE write instead of one interval.
   That is the correct mechanism for the residual and every decision point is in the hub's
   append path; it is recorded as work, not as a claim.

2. **No allocation ceiling for RSS — refused on merit.** Three mechanisms exist and all three
   destroy the property this ADR was written to protect. (i) Allocation denial (mosquitto's
   `memory_limit`) needs a custom `GlobalAlloc`, and the workspace sets
   `unsafe_code = "forbid"`; even with it, Rust's OOM path aborts by default and fallible
   allocation is unreachable across redb/tokio/rustls, so the delivered behaviour would be
   "abort somewhere" or "drop messages at malloc". (ii) EMQX's `force_shutdown` bounds a
   *per-connection* heap/mailbox; our dominant memory (offline queues, retained, hub maps)
   belongs to no connection, so it would cost a supervisor and still not bound the dominant
   term. (iii) The cgroup limit already IS the hard bound, and an OOM-kill under it is
   recoverable by design (ADR 0044). The exit is therefore documentation — but only counts as
   an exit because the docs now carry the arithmetic: the overshoot formula with worked
   instantiations, the watermark = 75-85%-of-container-limit mapping (the remaining 15-25% IS
   the overshoot allowance), two alert rules with thresholds, and the honest note that the
   Helm chart ships `resources: {}` so nothing sets that limit for you.

3. **The disk mark stays AGGREGATE — argued, with the visibility gap closed.** A per-store
   *share of the budget* is not enforceable as "refuse the growth writes to the over-share
   store" for half the enumeration: `retained` maps to a refusable client write, `sessions`
   maps partially (three growth writes are already deliberately exempt), but `replicas.redb`
   grows from other nodes' committed appends and `lease.redb` from consensus — a follower
   refusing committed entries would silently thin its group's replica count, which is the
   `min_replicas` floor's business, not a watermark's. Since the protected resource is one
   filesystem, the aggregate is the honest enforcement point. What the operator actually
   lacked — *which* store is eating the budget — is closed in code (the 70%/60% skew WARN)
   and in the docs (a per-store alert rule with numbers). **T9 is narrowed** to selective
   refusal for `sessions`/`retained` only, and blocked on giving `BrownoutAxis` a store
   dimension in the hub; `replicas`/`lease` map to the global axis by decision, stated so
   the semantics cannot become "some stores are enforceable and we never said which".

## Amendment (2026-08-14): a v5 DISCONNECT with a non-zero reason fires the Will (issue #265)

The T12 close-honesty seam (`PacketOutcome` — only a client DISCONNECT maps to a graceful
detach, so every broker-initiated close fires the Will, [MQTT-3.14.4-3]) had one
will-suppressing close left: the `Packet::Disconnect` arm ignored the v5 reason byte, so a
client DISCONNECT with `0x04 Disconnect with Will Message` — the code whose entire purpose
is "publish my Will" — or with any error reason was treated as reason `0x00` and its Will
discarded. Per [MQTT-3.1.2-10] only reason `0x00` discards the Will.

Since issue #265 the arm branches on the reason: `0x00` stays `ClientDisconnect`
(graceful, Will discarded); any non-zero reason is the new
`ClientDisconnectWithWill` — the socket close is equally clean, but the detach is
un-graceful so the Will fires. v3.1.1 is untouched: its DISCONNECT has no reason byte and
always decodes as `0`. The graceful-shutdown drain (ADR 0019) remains the one deliberate
close that is graceful without a client DISCONNECT — the server is going away, not the
client; the session is retained and the Will withheld.
