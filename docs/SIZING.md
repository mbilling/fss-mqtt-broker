# Sizing a node — the operational unknowns

**Who this is for:** an operator putting mqttd on a machine with a known memory and
disk budget, deciding which limits to set before exposing it to real load.

**The honest headline:** mqttd's defaults are *safe against attackers but not against
success*. Authentication is deny-by-default, packets are capped at 1 MiB, and every
quota refuses loudly at the edge (ADR 0041 — reason codes and backpressure, never
silent drops). But connections, sessions, retained topics, and disk are all
**uncapped until you cap them**, and the per-session offline queue defaults to 100 000
messages. Memory has a **watermark** (`MQTTD_MEMORY_MAX_BYTES`) but no hard ceiling:
crossing it degrades the broker to read-mostly, it does not stop memory rising. So the
arithmetic on this page is still what sizes the machine — the watermark is what tells you
the arithmetic was wrong, before the OOM killer does.

A ready-made preset with this page's numbers: [examples/bounded-node.toml](examples/bounded-node.toml).

## The minimum decisions

Eight numbers bound a node. Everything else has a safe default.

| Decision | Knob (env / TOML `[limits]` unless noted) | Ships as |
|---|---|---|
| Where state lives | `MQTTD_DATA_DIR` (`[node]`) | unset with durable on = **refused at startup** (issue #240) unless `MQTTD_ALLOW_EPHEMERAL_DURABILITY` opts into all-in-memory |
| Disk high-water mark | `MQTTD_STORE_MAX_BYTES` (`[durable]`) | unset = **no bound** |
| Concurrent connections | `MQTTD_MAX_CONNECTIONS` (+ `_PER_IP`) | unset = **uncapped** |
| Largest packet | `MQTTD_MAX_PACKET_SIZE` | 1 MiB (floor 1 KiB) |
| Sessions the node will remember | `MQTTD_MAX_SESSIONS` | unset = **uncapped** |
| Offline queue depth per session | `MQTTD_MAX_QUEUED_MESSAGES` + `MQTTD_QUEUE_OVERFLOW` (`drop-oldest` / `reject-newest`) | 100 000 / `drop-oldest` |
| Retained topics | `MQTTD_MAX_RETAINED_MESSAGES` | unset = **uncapped** |
| Brute-force cost | `MQTTD_AUTH_PENALTY_THRESHOLD` (+ `_DECAY_SECS`) | unset = **unlimited attempts** |

Two more worth a conscious pass: `MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT` (uncapped) and
`MQTTD_MAX_PUBLISH_RATE` (unlimited; enforcement is a socket-read pause — TCP
backpressure — not a drop or disconnect).

## Memory: the formula

There is no `memory_limit` in the allocation-denial sense. Size RSS from the caps you set:

```
RSS ≈ base (~70 MiB idle + ~15 KiB per idle connection, measured dev-grade in bench/)
    + connections × max_packet_size            (read buffering, worst case)
    + sessions × queued_messages × avg_msg     (offline queues — THE dominant term)
    + retained_topics × avg_retained_value
    + flow-control backlog: up to 10 000 msgs × avg_msg per slow live subscriber
```

The offline-queue term is why the 100 000-message default must be re-decided on a
bounded node: it is sized for *one important session*, not for thousands. **The queue
caps are message counts, not bytes** — the byte a message costs is whatever
`max_packet_size` allows, so the two knobs only bound memory *together* (a byte-based
cap, `max_queued_bytes`, is accepted work — ADR 0041 amendment T6).

Fixed internal bounds you get for free (hard-coded, not configurable — listed so your
arithmetic can include them):

| Internal bound | Value |
|---|---|
| Flow-control backlog per session | 10 000 msgs, drop-oldest |
| Replay to a resuming session | 10 000 msgs |
| Pending publishes awaiting durability | 4 096, ack withheld |
| Durable-append lane per session (issue #242) | 256 jobs (appends and QoS 2 outbound-id records share the cap), reject-newest (ack withheld), plus 16 reserved control slots for detach-spill/discard jobs; payload bytes are refcounted clones of the pending entry's, so the added cost per job is the message envelope, not a second payload copy |
| Retained mutations queued during heal (ADR 0037 §5) | 1 024, drop-oldest, counted |
| Peer-link read buffer | 32 MiB per peer |
| Peer frame / raft RPC / SWIM datagram | 16 MiB / 4 MiB / 64 KiB |

Run the broker under a container memory limit sized to the formula plus slack. The
durable design makes an OOM-kill recoverable (acked state is on disk/quorum and
crash-recovery is continuously tested, ADR 0044), but recovery is not free — the
point of the arithmetic is to not get there.

### The memory watermark (`MQTTD_MEMORY_MAX_BYTES`)

Set it below the container limit — 75-85% is a reasonable start — and crossing it puts
the broker into **brownout**: writes that *grow* state (new sessions, new retained
topics, offline enqueues) are refused with the ordinary quota reason codes, while
subscriber acks, reads, deletes, expiry and session resumes continue. A `QoS` ≥ 1
publisher whose durable enqueue is refused is **refused with it** — v5 `0x97`, v3.1.1 no
ack and a close, cross-node too as a peer-bus verdict (an older link mid-rolling-upgrade
withholds the ack and closes instead) — rather than acked for a message that was never
stored (issue #238).
Re-sending is the application's decision, not a protocol guarantee: a v5 reason ≥ `0x80`
completes the packet-id lifecycle and a clean-session v3.1.1 publisher resends nothing.
Dropping back under restores growth.
It is the same mechanism as the disk watermark, on a second axis; brownout is active
while **either** is over.

```
memory_max_bytes = 2147483648      # 2 GiB watermark…
                                   # …under a 2.5 GiB container limit
```

Three things it is not:

- **Not a ceiling.** Nothing here can stop memory rising. A burst that outruns the 10 s
  poll can still OOM. The container limit remains the hard bound; this buys you a metric,
  a log line and a degraded mode *before* that, in the common case where pressure builds
  over minutes.
- **Not allocation denial.** Mosquitto's `memory_limit` fails allocations at a heap cap
  and EMQX's `force_shutdown` kills the connection process. Both destroy standing state.
  Brownout refuses new growth and keeps everything already promised.
- **Not portable.** RSS is read from `/proc/self/status`. Released binaries are Linux-only;
  elsewhere the broker logs — loudly, at WARN, if you configured a watermark — that it is
  **not enforcing** one, rather than pretending.

Watch it with `process_resident_bytes / memory_max_bytes`, and alert on
`brownout{axis="memory"} == 1`.

## Disk: the formula

```
disk ≥ store_max_bytes + headroom (WAL/compaction slack + logs; ~20% is a sane start)
```

`MQTTD_STORE_MAX_BYTES` is **one aggregate high-water mark** across the four stores
(`sessions`, `retained`, `replicas`, `lease` — polled every 10 s, exported as
`store_bytes{store}`). Crossing it triggers **brownout**, not a stop: growth writes
are refused: a new session (counted in `quota_rejections_total{reason="brownout"}`),
a new retained topic (counted in `quota_rejections_total{reason="retained"}` — the
same label the retained quota uses), and an offline enqueue — for which a
`QoS` ≥ 1 publisher is **refused rather than acked** (v5 `0x97`, v3.1.1 no ack and a
close — cross-node too, as a peer-bus verdict, an older link mid-rolling-upgrade
degrading to a withheld ack + close; counted in
`quota_rejections_total{reason="brownout-publish"}`, issue #238).
Subscriber acks, reads, deletes and expiry continue, and dropping below the mark restores
writes. It is a high-water mark, not a hard wall — hence the headroom. And "growth is
refused" is not airtight: session **metadata** still grows under brownout (SUBSCRIBEs,
the inbound `QoS` 2 dedup window, detach spills of already-accepted messages — each
protects an honesty property worth more than its bytes), so a sustained brownout with
active clients grows the sessions store slowly. Size the watermark with real headroom
below disk-full, never at it.

Past the headroom, actual disk-full **fails closed** by the same rule brownout now
follows: a write that cannot be made durable withholds or refuses the publisher's ack
(nothing is silently lost; re-delivery is the publishing application's decision). This is crash-tested in the harshest form — the kernel killing the process
mid-write — with recovery and back-fill verified (ADR 0044 P2). The watermark is an
**aggregate** over all four stores: there is no per-store quota yet, so one store can
consume the whole budget and brown out the others (ADR 0041 amendment T9).

If `MQTTD_DATA_DIR` is unset, none of this applies — all state is in memory and the
memory formula is the only budget. (Reaching that state now takes an explicit choice:
durable-on with no data dir refuses to start unless `MQTTD_ALLOW_EPHEMERAL_DURABILITY`
is set, issue #240.)

### What actually writes to disk on the publish path

One store write per **QoS 1/2 message per matching persistent subscriber** — whether
that subscriber is offline *or* connected. The durable record is what the publisher's
PUBACK promises, so it is written before the message goes on the wire; the entry is
released when that subscriber acknowledges (issue #124).

That makes the write rate, not the resting size, the thing to size for:

```
store writes/s ≈ Σ over QoS 1/2 publishes ( matching PERSISTENT subscribers )
```

Three cases cost nothing: **QoS 0** at any time, any message to a **clean session**
(`clean_session=true` / zero Session Expiry — it has nothing to resume into), and any
message to a subscriber with no matching persistent session at all. A fan-out of one
QoS 1 publish to 100 persistent subscribers is 100 durable writes; to 100 clean ones it
is zero. In a cluster each of those writes is also quorum-replicated (R=3 by default),
so it costs a cross-node round trip as well.

If a topic's subscribers do not need redelivery across a broker restart, connecting them
with a clean session is the whole optimisation — there is no flag to turn durability off
for a session that asked for it, by design.

**Where a slow write is felt** (issue #242 / ADR 0061): these writes — the durable
append AND the QoS 2 outbound-id record that precedes an online wire send, AND the
once-per-1024-deliveries packet-id block reservation — run in per-session append lanes
off the hub loop, so replication latency no longer sets the pace for other clients.
The bounded worst case under a degraded follower set is per *session*: each of that
session's lane jobs is bounded by the 5 s replication RPC timeout, FIFO in its lane
(up to 256 queued jobs, shared between appends and outbound-id records — worst case
~21 min of retrying backlog for one session before new publishes to it are withheld
and retried by their publishers). Online QoS 2 delivery to a degraded group's
subscriber is additionally serial per subscriber: one 5 s-bounded record write per
message before its wire send (unchanged from the pre-ADR bound; the serialization
moved from the shared loop to that session's lane). Publishes to other groups'
sessions, connects, and subscribes are unaffected; hub dispatch time is independent
of replication latency on the publish and delivery paths (residual store awaits stay
in the ack/attach/control classes, ADR 0061), and `mqttd_hub_dispatch_seconds` /
`mqttd_append_lane_jobs` are the observables (see
[OPERATIONS](OPERATIONS.md#monitoring-for-the-operator-and-humans)).

## Worked example — 4 GiB RAM / 20 GiB disk

Target: leave ~1 GiB for OS/page cache; broker budget ~3 GiB RSS, 16 GiB store.

```
max_connections    = 2000     → 2000 × 256 KiB packets      ≈ 0.5 GiB worst-case buffering
max_packet_size    = 262144
max_sessions       = 5000
max_queued_messages= 500      → 5000 × 500 × 4 KiB avg      ≈ up to 9.8 GiB *on disk* (durable),
                                bounded in RAM by live-session working set; keep avg_msg honest
max_retained       = 50000    → 50000 × 4 KiB               ≈ 0.2 GiB
store_max_bytes    = 16 GB    → brownout at 16 GB, 4 GB headroom on the 20 GB disk
```

The full commented preset: [examples/bounded-node.toml](examples/bounded-node.toml).
Verify your numbers under load with the bench harness (`bench/run.sh`) and watch
`store_bytes`, `quota_rejections_total`, and RSS.

## What cannot be bounded today

Printed here so nobody discovers it in production:

| Axis | What bounds it today | Tracked by |
|---|---|---|
| Total process RSS **hard cap** | Watermark + brownout (`MQTTD_MEMORY_MAX_BYTES`, above); the container limit is still the ceiling | — by design |
| Offline queue **bytes** | Message count only (`MQTTD_MAX_QUEUED_MESSAGES`) | 0041-T6 |
| Bridge-spool **bytes** | Message count only (default 10 000) | 0041-T7 |
| Per-store disk share | Aggregate watermark only | 0041-T9 |
| Per-connection **write** buffering | Two hard-coded count caps, neither in bytes and neither configurable: `MAX_BACKLOG` (10 000 messages, QoS 1/2, drop-oldest) and the outbound queue (10 000 packets, QoS 0, shed and counted) | 0041-T10 |

The last row is the sharpest edge: a subscriber that stops reading can hold up to
10 000 messages, and at the 1 MiB default packet size that is ~10 GiB of headroom per
connection that no setting can lower. Cap `MQTTD_MAX_PACKET_SIZE` to bound it in
practice — the product of the two is the real worst case.

All five are recorded losses in [COMPARISON.md](COMPARISON.md).
