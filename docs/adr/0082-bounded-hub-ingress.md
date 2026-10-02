# 0082. Bounded hub ingress: a control lane that never waits behind data, and byte credits that push back on publishers

- **Status:** Accepted
- **Date:** 2026-10-01 (proposed), 2026-10-02 (accepted)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0082-bounded-hub-ingress.md](../delivery/0082-bounded-hub-ingress.md) — plan, progress, and changelog
- **Related:** [ADR 0041](0041-resource-governance.md) (resource governance: what a brownout
  refuses, the outbound and backlog caps), [ADR 0061](0061-off-loop-durable-appends.md)
  (per-session append lanes, already bounded), [ADR 0015](0015-cluster-shared-subscriptions.md)
  (`$share` delivery and its capacity fallback), [ADR 0020](0020-metrics-and-observability.md)
  (metrics and label cardinality). Issues: #535 (the queue-budget design this ADR is),
  #504 (overload non-recovery, whose acceptance this closes), #509 (the pending-publish table,
  which stays its own owner).

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0082-bounded-hub-ingress.md).

## Context

The hub is one task with one command channel, and that channel is unbounded
(`crates/mqttd/src/hub/mod.rs:2382`). Every client connection, every peer link and the hub
itself push into it with `send`, which never waits:

- **Client publishes:** `conn.rs` `handle_inbound` reads a PUBLISH and calls
  `hub.send(HubCommand::Publish { … })` (`conn.rs:2147`). A QoS 0 publish carries no
  completion and nothing slows the read loop down.
- **Peer data:** the peer reader forwards remote publishes, shared deliveries and retained
  updates the same way (`peer.rs:682-919`).
- **The durable plane rides the same channel.** Raft RPCs, replication and replica reads
  arrive as `HubCommand::DurableFrame` (`peer.rs:901-919`). So do completions, attach and
  detach, acks, admin requests, and the `Ping` that `/livez` waits on (`health.rs:43`, 2 s).

### What failed, measured

The #504 cloud acceptance runs on 2026-10-01 put a 3-node cluster at 1.5× capacity (QoS 0
`$share`, 540k msg/s offered).

**First run:** one broker froze at its cgroup `MemoryHigh` (10 GB) with 15M QoS 0 frames on
its peer links. #811 bounded that path.

**Re-run** (candidate c7fc480, including #811):
- **Brokers 0 and 2 froze** at 10.7 GB anonymous RSS. Every tokio worker was in state `D`
  in `mem_cgroup_handle_over_high` (throttled, not deadlocked), and each still held about
  7,200 dead client sockets.
- **Broker 1 stayed under the limit and recovered on its own.** Its scrapes name the queue:

| Scrape | `mqttd_hub_queue_depth` | RSS |
|---|---|---|
| Baseline (6 sites) | 0 | 108 MB |
| Rung 18, window open | **2,553,413** | 2.60 GB |
| Rung 18, window close (drained) | 705 | 275 MB |

That is about **1,019 bytes per queued command** at 200 B payloads. The hub dispatched
46.85M publishes in 184.6 s, about 3.9 µs each. So 2.5M queued commands is also about
**10 s of latency for anything behind them**, raft votes included. That is the 1.5 s
vote-timeout churn and the SWIM flapping the first run logged before its broker went silent.

The memory brownout (ADR 0041 T8, `MQTTD_MEMORY_MAX_BYTES`) does not help here. It refuses
*growth*: new sessions, new retained topics, offline enqueues. A QoS 0 publish passes
straight through into the queue. Production's shipped unit has `MemoryHigh=1500M`, so it
reaches the same wall about seven times sooner.

### Queue inventory

| Queue | Where | Bound today |
|---|---|---|
| **Hub command channel** (all producers) | `hub/mod.rs:2382` | **none** ← the 10 GB |
| Client outbound per subscriber | `hub/mod.rs:387` `MAX_OUTBOUND_QUEUE` (10,000) + `MQTTD_MAX_OUTBOUND_BYTES` | yes (QoS 0 shed, counted) |
| Flow-control backlog per session | `backpressure.rs:86` (10,000) + `MQTTD_MAX_BACKLOG_BYTES` | yes |
| Peer link lanes, QoS 0 | `peer.rs:470,478`, `PEER_QOS0_BACKLOG_CAP` (100,000) | yes (#811, shed counted) |
| Peer link lanes, QoS ≥ 1 forwards | same lanes | by the origin's pending-publish table |
| Pending-publish table | `hub/mod.rs:2319,2327` (65,536 / 64 MiB) | yes (#509 owns the design) |
| Per-session append lanes | `hub/lanes.rs:217` `LANE_QUEUE_CAP` (256) + control headroom | yes |
| Store truncate queue | `hub/mod.rs:2660` | unbounded, one entry per acked offset; small |
| Retained handoff queues | `hub/retained.rs` (#798) | capped, drop-oldest |
| Bridge spool | `mqtt-bridge` (#540) | by bytes |

The hub command channel is the only unbounded queue whose producers are external and
unthrottled. Everything downstream of it is already bounded.

## Decision

### 1. Two lanes into the hub: control first, data second

The single channel becomes two, and the hub loop polls them `biased`, control first.

- **Control:** everything whose volume is bounded by something other than the publish rate,
  or whose delay breaks a protocol. That covers:
  - attach, detach, evict;
  - PUBACK, PUBREC and PUBCOMP from clients;
  - lane and store completions (`AppendDone` and the like);
  - `DurableFrame` (raft, replication, replica reads);
  - membership and interest gossip;
  - retained commits, acks and snapshots;
  - remote acks and verdicts;
  - `Ping`, admin, quota and brownout settings.
- **Data:** client `Publish`; remote `RemotePublish` and `RemoteSharedDeliver`; and
  `RemoteRetainedUpdate`, which is a cache update that converges by digest.

The control lane stays unbounded. Its producers are bounded by connection count, by
in-flight windows, or by the hub's own outstanding work, and a bounded control lane is
exactly where a credit cycle would form (the hub awaiting capacity that only its own
dispatch frees). Under any data backlog, a raft RPC, a detach or a `/livez` ping waits
behind control traffic only.

### 2. Client publishes acquire byte credit; a connection without credit stops reading

There is one node-wide **byte pool** (`MQTTD_HUB_INGRESS_BYTES`) and a **per-connection
cap** (`MQTTD_CONN_INGRESS_BYTES`).

- **Acquire:** after a client PUBLISH is read and decoded, its handler acquires `cost`
  bytes, first from its connection's allowance and then from the pool, before handing the
  command to the data lane.
- **Charge:** `cost` is the topic length plus the payload length plus a fixed per-command
  overhead calibrated in T1, so the pool bounds *retained* memory, not just payload bytes.
  As calibrated: `2 × size_of::<HubCommand>() + 384` (880 B since #835), with the properties
  charged alongside the payload (T1 amendment below).
- **Release:** the permit (an owned semaphore permit) **travels inside the command** and is
  released when the hub drops it after dispatch. The hub never acquires credit, so no
  credit cycle can form.
- **Pausing:** while a connection waits for credit it **does not read its socket**. The
  kernel's receive buffer fills, TCP closes the window, and that publisher slows to the rate
  the hub retires its work. This is lossless at every QoS and costs nothing per message.
- **Fairness:** the pool's semaphore is FIFO, so waiting connections are served in order.
  The per-connection cap stops one hot publisher from holding the whole pool.
- **Oversize:** a single message larger than the cap is clamped to the cap, so it can
  always eventually proceed. `MQTTD_MAX_PACKET_SIZE` already bounds the maximum.

**QoS 1/2 are unchanged.** Nothing is acknowledged before it is read, so pausing the read
accepts no obligation. Receive Maximum is not renegotiated. Durable work already accepted
keeps its existing acks and refusals.

**Keepalive:** a connection's own keepalive deadline does not run while it is paused by the
broker. A client whose PINGREQ goes unread past its own timeout will reconnect, which is
the honest signal that the broker is saturated. It happens only under sustained overload,
and the reconnect lands in the same credit queue.

### 2a. What a client connection does past its credit is configurable; pausing is the default

`MQTTD_INGRESS_OVERLOAD` (`[limits] ingress_overload`) selects the behaviour for
**client QoS 0** past the credit:

- **`pause`** (default): the connection stops reading, as in §2. It is lossless. Under
  sustained overload, a client with a short PINGREQ timeout reconnects.
- **`shed-qos0`**: the connection keeps reading. A QoS 0 publish that cannot get credit is
  dropped and counted as `publish_dropped{reason="hub-ingress"}`. Clients stay connected,
  but QoS 0 messages a slower read would have kept are lost, and the broker still pays the
  read and decode of every dropped publish, so CPU is not bounded the way memory is.

**QoS 1 and 2 pause under either setting.** A publish the broker will acknowledge, or
already has, is never shed to satisfy a memory bound. The setting is hot-reloadable with
the other limits.

### 3. Peer data is shed, never paused

A peer link carries raft and replication frames in the same TCP stream as data. Pausing its
reads to bound memory would stall consensus, which is the failure mode this ADR exists to
remove. So peer data uses non-blocking admission:

- **Remote QoS 0** (forwards and shared deliveries) tries for pool credit. Without it, the
  message is dropped and counted as `publish_dropped{reason="hub-ingress"}`. QoS 0 promises
  nothing, and the same drop already happens at the outbound and peer-backlog caps.
- **Remote QoS ≥ 1 is not charged.** The origin's pending-publish table already bounds how
  many of these exist per origin (#509). Charging them would turn a bounded obligation into
  a refusal path with version-skew cost, for no memory gain.
- **A retained forward is not charged, whatever its QoS** (T4, 2026-10-02). It is this
  node's only copy of the topic's retained state: shedding it would leave the node serving
  a stale retained message until the next one, which outlasts the overload. Retained
  updates are rare beside live traffic.
- **Control and durable frames are never charged.**
- The pool is the node's, with no per-link cap: one link carries many publishers. The
  reader counts each shed on the shared credit, and the hub's once-a-second sweep moves the
  count into the counter, so the reader needs no metrics handle.

### 4. What an operator sees

All new series have bounded label cardinality. As built (names corrected 2026-10-02, T5):
- `mqttd_hub_lane_depth{lane="control|data"}` (T2), beside the existing total
  `mqttd_hub_queue_depth`;
- `mqttd_ingress_credit_bytes{state="in_use|capacity"}` (T3): queued bytes against the pool;
- `mqttd_ingress_paused_total` and the `mqttd_ingress_paused_seconds` histogram (T3):
  connections that waited for credit, and for how long;
- `publish_dropped{reason="hub-ingress"}`: peer QoS 0 shed (T4), and client QoS 0 under
  `shed-qos0` (T3).

The runbook and the shipped alerts are in [OPERATIONS](../OPERATIONS.md#overload-bounded-hub-ingress-adr-0082) (T5).

`/livez` keeps answering under overload because its ping is control traffic.

### 5. Defaults

- **`MQTTD_HUB_INGRESS_BYTES`:** 1/8 of `MQTTD_MEMORY_MAX_BYTES` when that is set, otherwise
  256 MiB. At about 1 KB per command, that is roughly 250k queued commands, or about a
  second of hub work. So the bound also caps the added latency, not only memory.
- **`MQTTD_CONN_INGRESS_BYTES`:** 1 MiB.

Both are hot-reloadable as limits. SIZING.md states the arithmetic against the shipped
unit's `MemoryHigh=1500M`.

### 6. No wire change

Everything here is local admission. Peer QoS 0 shedding already exists as a behavior
(#811), and remote QoS ≥ 1 is untouched, so there is no protocol bump and no
version-skew window.

## Consequences

**Good:**
- A node's memory under ingress overload is bounded by configuration, not by the offer.
- The process keeps scheduling, so overload ends and the node recovers without a restart.
- Consensus, cleanup and health stay live under data saturation, because they no longer
  queue behind publishes. That removes the leader churn seen under load.
- Publishers are slowed by TCP, which every MQTT client already handles.

**Bad:**
- Under sustained overload, publishers see backpressure rather than acceptance. Clients
  with short PINGREQ timeouts reconnect.
- Remote QoS 0 is shed at a second place, the hub ingress (counted).

**Neutral:**
- The per-command charge is a calibrated estimate, re-checked by T1's measurement whenever
  the command layout changes.
- `hub_queue_depth` changes from one series to two.

## Alternatives considered

- **Bound the single channel with blocking `send`.** Rejected: the hub sends to itself and
  peer readers carry raft frames. One full channel would deadlock the hub on its own
  completions and stall consensus.
- **Shed client QoS 0 at ingress instead of pausing, as the only behaviour.** Adopted as
  the opt-in `shed-qos0` setting (§2a), not the default. This keeps clients connected but
  loses messages a slower read would have kept. It also still pays read and decode for every
  dropped publish, so it doesn't bound CPU. It is kept as a possible future knob, not the
  default.
- **Refuse client QoS 1/2 with a reason code when over credit.** Unnecessary: an unread
  PUBLISH carries no obligation, so pausing is lossless where a refusal makes the client
  resend.
- **Rely on the memory brownout (ADR 0041 T8).** It refuses growth, not publishes, and it
  reacts to RSS after the fact. The hub queue fills between samples.
- **Credit per publisher via MQTT flow control.** Receive Maximum can't be changed
  mid-connection, and QoS 0 has no flow control at all.
- **Shard the hub.** Raises capacity but doesn't bound memory. Any shard can still be
  outrun.

## Decision record (2026-10-02)

The maintainer accepted this ADR with three answers to the questions the proposal left open:

1. **Overload behaviour is configurable, with `pause` as the default** (§2a,
   `MQTTD_INGRESS_OVERLOAD=pause|shed-qos0`). QoS 1/2 pause under either setting.
2. **The defaults stand as proposed:** the pool is 1/8 of `MQTTD_MEMORY_MAX_BYTES` (or
   256 MiB), and the per-connection cap is 1 MiB.
3. **T2 ships on its own first:** the lane split, with no admission change. The credits (T3)
   follow.

## Amendment (2026-10-02): which commands are control, as built in T2

§1 listed attach, detach, membership, interest gossip, retained snapshots and policy
changes as control. Implementing T2 showed that each of those changes what an
**earlier-queued** data command does, so letting it overtake them changes behaviour:
- a client's DISCONNECT overtaking its own PUBLISHes fires its will first;
- a brownout flip overtaking a publish refuses something admitted under the old policy
  (#238);
- a peer's link-up overtaking retained publishes offers a digest and drains a handoff
  queue those publishes had not built yet;
- a retained snapshot or digest overtaking a retained publish leaves it out.

So the rule, as built (`HubCommand::lane`), is: **a command is control only if nothing
queued before it can change what it does.** Control is completions and acks (lane, store
and retained-commit completions; client PUBACK, PUBREC and PUBCOMP; a peer's forward ack
and verdict), the durable plane (raft and replication frames), the `/livez` ping and
admin. Everything else stays ordered on the data lane. The goals of §1 still hold:
consensus frames, acks and health never wait behind publishes. Session lifecycle,
membership and policy changes keep arrival order, as before the split.

Two further details:
- **A data-lane barrier.** `HubCommand::Flush` replies once everything sent before it, in
  either lane, is dispatched. `Ping` now answers as soon as the control lane is clear,
  which is right for `/livez` and wrong for a flush.
- **Metrics.** `mqttd_hub_queue_depth` keeps its meaning (everything queued, now the
  channel plus both lanes) so existing dashboards hold. The per-lane split is a new
  family, `mqttd_hub_lane_depth{lane="control|data"}`.

## Amendment (2026-10-02): client credit, as built in T3

§2 and §2a stand, with these details settled in the implementation (`crates/mqttd/src/ingress.rs`
and the connection's serve loop):
- **Where credit is taken.** After the publish-rate limiter (ADR 0041 T3), so a throttled
  publish holds no credit while it waits for a token. Only a PUBLISH read from a client socket
  is charged; every other packet passes uncharged.
- **The charge.** `topic + payload + 800` bytes. The 800 comes from #504's measured 1,019 B per
  queued command at 200 B payloads, and stays until T1 measures it directly. (Superseded by
  the T1 amendment below.)
- **A paused connection keeps working.** The parked publish waits in its own `select!` branch,
  so deliveries to the client, PUBACK release and shutdown all continue while reading is
  stopped. The keepalive timer is disarmed while parked and restarts when reading resumes.
- **Shedding happens after the connection's own bookkeeping.** Under `shed-qos0`, a QoS 0
  publish without credit still has its topic alias registered, its topic validated and the ACL
  applied. Only the hand-off to the hub is skipped, so later publishes that use the alias stay
  valid.
- **Restart-scoped, not hot-reloadable.** §2a said the setting reloads with the other limits.
  In fact the whole `[limits]` section is restart-scoped (ADR 0041 §6), and the pool is built
  once at startup. The three settings follow the rest of the section.
- **Metrics.** `mqttd_ingress_paused_total` (a counter), `mqttd_ingress_paused_seconds` (a
  histogram rather than §4's `_seconds_total` counter, so the length of a pause is visible,
  not just the sum), and `mqttd_ingress_credit_bytes{state="in_use|capacity"}` in place of
  §4's `mqttd_hub_ingress_bytes`. The hub exports the gauge on its sweep. Client QoS 0 shed
  under `shed-qos0` counts as `publish_dropped{reason="hub-ingress"}`, the reason T4 will use
  for peer QoS 0.

## Amendment (2026-10-02): a paused connection still notices its client leaving (#825)

T3 left a gap that the #504 cloud acceptance (T6) measured: a paused connection does not
read its socket, so a client's FIN queued behind unread publishes went unseen until credit
freed. With keepalive disarmed, nothing else reaped it. At 4× overload on one node, hundreds
of such connections outlived the reset budget.

- **The watch.** While a publish is parked, the serve loop also waits for the transport to
  report the peer gone (`conn::PeerClosedWatch`). It never reads payload, so backpressure
  is unchanged:
  - **TCP, TLS, WS, WSS:** the watch is built from the raw accepted socket before TLS or
    WebSocket wraps it. It registers a duplicate descriptor for read-closed readiness only:
    `EPOLLRDHUP` on Linux, `EV_EOF` on macOS. The kernel raises it on the peer's FIN or RST
    however much data is still buffered. Plain data-arrival wakeups are edge-triggered and
    waited past, so they do not spin.
  - **QUIC:** the watch is the connection's close, which quinn raises on the client's
    CONNECTION_CLOSE or its own idle timeout whether or not the stream is read.
  - **Not watched:** non-unix targets, which keep the T3 behaviour, and sessions proxied
    from a relaying node, whose stream is the peer link, not the client socket.
- **What happens on a hangup.** The same as reaching EOF without DISCONNECT: the close is
  ungraceful and the Will fires. The parked publish and anything unread behind it are
  dropped and never acknowledged; the parked publish never held credit. A client that
  queued a DISCONNECT behind its publishes and then closed is treated the same way: the
  broker never read the DISCONNECT. Before this change that client's publishes would have
  been served once credit freed.
- **Keepalive stays disarmed while paused.** The broker caused the silence, and a client
  whose writes are blocked by TCP flow control cannot get a PINGREQ through. Re-arming
  keepalive would disconnect healthy clients exactly when the node is busiest. The watch
  covers clients that close or reset. A client that vanishes without either (power loss, a
  dropped network path) is still reaped by keepalive once reading resumes, so it stays
  bounded by the pause plus the grace period.

## Amendment (2026-10-02): the charge, calibrated in T1

**The measurement.** `crates/mqttd/tests/ingress_cost.rs` runs publishes down the
connection's own path, under the release allocator (mimalloc):
1. the production `FrameReader` decodes framed PUBLISHes from a stream read in TCP-sized
   segments;
2. each becomes a `HubCommand::Publish` and is queued;
3. the test reads the RSS each queued command added.

Topic and payload bytes are pseudo-random, because macOS compresses idle pages and
identical bytes would read as almost nothing. Each case keeps its memory alive to the end,
so the allocator cannot recycle one case's pages into the next. The test asserts the charge
covers every case and is no more than 4× what is retained (3× before #835 shrank the slot). It fails if the overhead drops
to one slot.

| Where the command waits | Retained beyond topic + payload |
|---|---|
| on the hub's channel (408-byte `HubCommand`) | 455-610 B |
| in a hub lane (`VecDeque`) just past a doubling | 731-1,076 B |

**The lane is the case that matters.** Under overload the hub moves its backlog off the
channel into its lanes (T2), and a `VecDeque`'s capacity doubles. Just past a doubling, half
its slots are empty, so one command can cost two 408-byte slots. Hence:

**`COMMAND_OVERHEAD = 2 × size_of::<HubCommand>() + 384`**, which is 1,200 B today. It is
derived from the slot, so the charge follows the command if the command grows. The 800 it
replaces undercharged the lane case by up to about 25%. At the default 256 MiB pool and
200-byte payloads, the pool now holds about 190,000 publishes, not 263,000. The cloud
measurement it came from, 1,019 B per command, divided total RSS, baseline included, by the
queue.

Two undercharges are also fixed:
- **Properties are charged with the payload**, using the same `accounted_bytes` the
  subscriber backlog bound uses (#241). These bytes are publisher-controlled and held as
  long as the payload: a 20-byte payload with 8 KiB of user properties was charged as
  20 bytes. Peer forwards charge their properties too.
- **An alias-only PUBLISH pays for the topic its alias stands for**
  (`InboundAliases::resolved_len`), not for its empty wire topic.

**The lanes shrink after an overload.** A `VecDeque` never shrinks on its own, so a lane
that once held the pool would keep about 200 MB, two slots per command at the default pool,
for the life of the process. The sweep now shrinks a lane that is three-quarters empty to
twice what it holds, never below 1,024 slots.

**The harness.** The process-level before / overload / idle / control sequence is the scale
rig's knee harness (`bench/scale/482-per-node-knee.sh` with `run-curve.sh`). It drives a
baseline rung, overload rungs, an idle and reset gate, and the same control rung, with no
restart. It asserts delivery, p99, reset and admission. It also scrapes every broker's
`/metrics` before and after each rung, which records RSS, the hub queue and credit in use,
and fails on an unanswered scrape. T6 ran it in the cloud. The per-command
cost above is the part CI can check on every PR.

**Follow-up.** The slot is the largest term: two of them are 816 of the 1,200 bytes. Boxing
`HubCommand`'s largest variants would shrink every queued command, and with it the charge.

## Amendment (2026-10-02): a smaller command slot (#835)

The T1 follow-up is done. `-Zprint-type-sizes` showed the 408-byte slot was set by rare
variants, not by publishes:

| Variant | Before | Largest field |
|---|---|---|
| `SessionRecovered` | 408 | `pending` 376 |
| `Attach` | 383 | `will` 200, `admission` 112 |
| `RetainedCommitDone` | 266 | `app` 112 |
| `Publish` | 251 | `app` 112 |

Those fields are now boxed:
- `SessionRecovered.pending`;
- `Attach.admission` and `Attach.will`;
- `RetainedCommitDone.app`.

Each is sent once per session recovery, connect or retained commit, so the extra
allocation is off the publish path. The slot is now **248 B**, set by `Publish`, and a test
pins it at 256 B or less.

The charge follows the slot. `COMMAND_OVERHEAD` falls from 1,200 B to **880 B**, and the
default 256 MiB pool now holds about 245,000 waiting 200-byte publishes, up from about
190,000. Each such publish holds about 200 B less memory.

Re-measured with `tests/ingress_cost.rs`:

| Where the command waits | Retained beyond topic + payload |
|---|---|
| hub channel | 256-412 B |
| hub lane, worst point | up to 552 B |

The charge still covers every case. The test's upper bound moves from 3× to 4×, because an
empty payload is now 896 B charged against 272 B held.

What remains is `AppProperties` at 112 B: every publish variant carries it, and shrinking it
would need a broad API change.

