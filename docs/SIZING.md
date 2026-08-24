# Sizing a node — the operational unknowns

**Who this is for:** an operator putting mqttd on a machine with a known memory and
disk budget, deciding which limits to set before exposing it to real load.

**The honest headline:** mqttd's defaults are *safe against attackers but not against
success*. Authentication is deny-by-default, packets are capped at 1 MiB, and every
quota refuses loudly at the edge (ADR 0041 — reason codes and backpressure, never
silent drops). But connections, sessions, retained topics, and disk are all
**uncapped until you cap them**, and the per-session offline queue defaults to 100 000
messages. Memory has a **watermark** (`MQTTD_MEMORY_MAX_BYTES`) but no hard ceiling:
crossing it degrades the broker to read-mostly, it does not stop memory rising — and
because the mark is *sampled* rather than charged at each allocation, the overshoot is
bounded only by `MQTTD_WATERMARK_POLL x your growth rate` (default 10 s; 1 s once within
10% of the mark). So the arithmetic on this page is still what sizes the machine — the
watermark is what tells you the arithmetic was wrong, before the OOM killer does.

A ready-made preset with this page's numbers: [examples/bounded-node.toml](examples/bounded-node.toml).

## The minimum decisions

Nine numbers bound a node. Everything else has a safe default.

| Decision | Knob (env / TOML `[limits]` unless noted) | Ships as |
|---|---|---|
| Where state lives | `MQTTD_DATA_DIR` (`[node]`) | unset with durable on = **refused at startup** (issue #240) unless `MQTTD_ALLOW_EPHEMERAL_DURABILITY` opts into all-in-memory |
| Disk high-water mark | `MQTTD_STORE_MAX_BYTES` (`[durable]`) | unset = **no bound** |
| Concurrent connections | `MQTTD_MAX_CONNECTIONS` (+ `_PER_IP`) | unset = **uncapped** |
| Largest packet | `MQTTD_MAX_PACKET_SIZE` | 1 MiB (floor 1 KiB) |
| Sessions the node will remember | `MQTTD_MAX_SESSIONS` | unset = **uncapped** |
| Offline queue depth per session (**disk**) | `MQTTD_MAX_QUEUED_MESSAGES` + `MQTTD_QUEUE_OVERFLOW` (`drop-oldest` / `reject-newest`) | 100 000 / `drop-oldest` |
| What one stalled subscriber holds (**RAM**) | `MQTTD_MAX_BACKLOG_MESSAGES` + `MQTTD_MAX_BACKLOG_BYTES`, and `MQTTD_MAX_INFLIGHT_MESSAGES` (a wire-window gate, not loss-free — see below) | 10 000 messages / byte bound **off** / in-flight window **65 535** |
| Retained topics | `MQTTD_MAX_RETAINED_MESSAGES` | unset = **uncapped** |
| Brute-force cost | `MQTTD_AUTH_PENALTY_THRESHOLD` (+ `_DECAY_SECS`) | unset = **unlimited attempts** |

Two more worth a conscious pass: `MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT` (uncapped) and
`MQTTD_MAX_PUBLISH_RATE` (unlimited; enforcement is a socket-read pause — TCP
backpressure — not a drop or disconnect).

**Take the per-subscriber row in this order.** `MQTTD_MAX_INFLIGHT_MESSAGES` first: it is a
pure gate on the in-flight window — the surplus waits in the backlog and **nothing is
dropped** — and unset it is the largest of the three structures at 65 535 entries.
`MQTTD_MAX_BACKLOG_BYTES` second, and knowingly: at that bound the broker truncates
already-acked messages out of a slow subscriber's backlog and **does not tell the
publisher**. Read `mqttd_backlog_bytes_max` before choosing a number (the largest single subscriber's backlog — `mqttd_backlog_bytes` is the node-wide SUM and would size the cap far too high).

## Memory: the formula

There is no `memory_limit` in the allocation-denial sense. Size RSS from the caps you set:

```
RSS ≈ base (~70 MiB idle + ~15 KiB per idle connection — see the note below)
    + connections × max_packet_size            (read buffering, worst case)
    + sessions × queued_messages × avg_msg     (offline queues — THE dominant term)
    + retained_topics × avg_retained_value
    + per slow live subscriber (RAM, issue #241 — all three now lowerable):
        flow-control backlog:  max_backlog_bytes + 2 × (max_packet_size + 256)   [byte cap set]
                             = max_backlog_messages × (avg_msg + 256)           [byte cap unset]
        in-flight window:      min(client Receive Maximum, max_inflight_messages) × avg_msg
                               [65 535 × avg_msg when the ceiling is unset]
        outbound channel:      max_outbound_bytes, else up to 10 000 packets × avg_msg
```

The three per-subscriber terms **over**-count rather than under-count: payloads are
refcounted, so a message counted in both the in-flight table and the outbound channel holds
one allocation. The `+ 256` is the per-entry envelope the byte accounting charges, and the
`2 ×` in the byte-capped row is the one entry that may exceed the whole cap (kept so
delivery still progresses) plus one already-admitted re-parked entry. `max_backlog_bytes` is
a bound on *message bytes held for one subscriber* — the sum of `256 + topic + payload +
forwarded application properties` over the resident entries — not an RSS measurement, which
is why this formula keeps its own slack rather than treating the cap as a ceiling.

> **Where the base term comes from, honestly.** The ~70 MiB / ~15 KiB figures are from an
> unpublished dev-grade run of `bench/` (its `results/` directory is untracked scratch), so
> they are **not** reproducible from this repository — treat them as an order of magnitude
> to size against, not a measured constant, and re-measure on your own host before they
> matter. They have not been replaced with a number from
> [benchmarks/DURABLE-PATH.md](benchmarks/DURABLE-PATH.md) on purpose: that harness runs on
> macOS, where `ps` RSS (compressed memory, shared pages) is not comparable to the Linux
> `VmRSS` this formula is written against. For reference only, the same three broker
> processes there sat at **10–20 MiB RSS each** while serving durable traffic — which says
> the base term is dominated by whatever an idle deployment configures, not by the broker's
> floor.

The offline-queue term is why the 100 000-message default must be re-decided on a
bounded node: it is sized for *one important session*, not for thousands. **The offline
queue's cap is a message count, not bytes** — the byte a message costs is whatever
`max_packet_size` allows, so those two knobs only bound *disk* together (a byte-based cap
for the offline queue, mosquitto's `max_queued_bytes`, is still accepted work — ADR 0041
amendment T6; issue #241 deliberately did not claim it, because getting it exact needs a
*persisted* per-session counter and a counter that drifts fires the cap at the wrong time).
The **in-memory** flow-control backlog is bounded in bytes as of issue #241
(`MQTTD_MAX_BACKLOG_BYTES`): that knob bounds RAM per online subscriber, per node, and
bounds **no disk** — though a byte eviction does release its entry's offset and truncate, so
it shrinks the durable log *earlier*.

Fixed internal bounds you get for free (hard-coded, not configurable — listed so your
arithmetic can include them). The flow-control backlog left this table in issue #241: it is
two knobs now (`MQTTD_MAX_BACKLOG_MESSAGES`, `MQTTD_MAX_BACKLOG_BYTES`), and the outbound
channel's *packet* count stays here while its *bytes* became `MQTTD_MAX_OUTBOUND_BYTES`:

| Internal bound | Value |
|---|---|
| Outbound socket channel per connection, **packets** | 10 000; `QoS` 0 over it is shed and counted (`publish_dropped{reason="outbound-full"}`), control packets and `QoS` 1/2 always flow. Its *bytes* are `MQTTD_MAX_OUTBOUND_BYTES` |
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

- **Not a ceiling — and here is the number.** Nothing here can stop memory rising. The
  mark is checked when the watcher samples RSS, never charged at an allocation, so

  ```
  overshoot ≤ poll interval × peak allocation rate   (+ the allocation in flight)
  ```

  The interval is `MQTTD_WATERMARK_POLL` seconds (default **10**, range 1-300) and
  `poll / 10` with a 1 s floor once RSS is **within 10% of the mark** — so at the defaults
  the bound is ~1 s of allocation in the band that matters and ~10 s from a standing
  start. Arithmetic on this page's own worked example, not a measurement: 2000
  connections × 256 KiB read buffers is 0.5 GiB that can be allocated well inside one
  10 s interval, which is why the container limit has to sit *above* the mark by more
  than the overshoot.

  **The mapping, as arithmetic.** Set the watermark to **75-85% of
  `resources.limits.memory`** — i.e. 15-25% of the container limit *is* the overshoot
  allowance. If `poll × your measured peak allocation rate` exceeds that gap, lower the
  poll or lower the watermark; measure the rate with `bench/run.sh` while watching
  `process_resident_bytes`. Deployment reality: the compose stack sets `memory: 1g` and
  explains the pairing (`deploy/compose/compose.yaml`), but **the Helm chart ships
  `resources: {}`** with the limits block commented out — set `resources.limits.memory`
  yourself, the chart will not do it for you.
- **Not allocation denial**, and that is a decision rather than a gap. Mosquitto's
  `memory_limit` fails allocations at a heap cap; EMQX's `force_shutdown` kills the
  connection process over a per-connection heap/mailbox bound. Both destroy standing
  state, which is the one thing brownout exists to keep. Concretely, for this codebase:
  allocation denial needs a custom global allocator and the workspace sets
  `unsafe_code = "forbid"`, and Rust's OOM path aborts by default — the delivered
  behaviour would be "abort somewhere" or "drop messages at malloc". A `force_shutdown`
  equivalent would need per-connection accounting we do not have, and the memory that
  actually dominates (offline queues, retained, hub maps) belongs to no connection, so it
  would not bound the dominant term anyway. The cgroup limit is the ceiling, and an
  OOM-kill under it is recoverable by design (acked state is on disk/quorum, ADR 0044).
- **Not portable.** RSS is read from `/proc/self/status`. Released binaries are Linux-only;
  elsewhere the broker logs — loudly, at WARN, if you configured a watermark — that it is
  **not enforcing** one, rather than pretending.

Watch it with `process_resident_bytes / memory_max_bytes`. Two rules, with numbers:

- **page** on `mqttd_brownout{axis="memory"} == 1` — growth is being refused right now,
  and `QoS` ≥ 1 publishers to topics with persistent subscribers are being refused with it;
- **warn** on
  `mqttd_process_resident_bytes / mqttd_memory_max_bytes > 0.9 and mqttd_memory_max_bytes > 0`
  for 5m — the accelerated-poll band, i.e. the last warning you get before brownout. **The
  `and` clause is load-bearing, not decoration:** with no watermark configured the gauge is
  exported as a literal `0` (not absent), and PromQL follows IEEE 754, so the bare ratio is
  `+Inf` and fires permanently on the default configuration. If you have no watermark,
  alert against the container limit instead.

## Disk: the formula

```
disk ≥ store_max_bytes + headroom (WAL/compaction slack + logs)
       headroom ≥ the overshoot bound below, and never less than ~20%
```

`MQTTD_STORE_MAX_BYTES` is **one aggregate high-water mark** across the four stores
(`sessions`, `retained`, `replicas`, `lease` — polled every `MQTTD_WATERMARK_POLL`
seconds, default 10, and every `poll / 10` with a 1 s floor once the total is within 10%
of the mark; exported as `store_bytes{store}`). The same overshoot bound as memory
applies, for the same reason — the mark is scanned, not charged at the write:

```
disk overshoot ≤ poll interval × your store growth rate   (+ the write in flight)
```

Measure the rate rather than guess it: run `bench/run.sh` at your intended publish mix and
watch `store_bytes` climb. As an *example* of the arithmetic (not a measured throughput —
this repo publishes none): 500 durable writes/s at 4 KiB is ~2 MB/s, so a 10 s poll costs
~20 MB of overshoot and a 1 s near-mark poll ~2 MB. If your rate makes that number
uncomfortable next to your headroom, lower `MQTTD_WATERMARK_POLL`.

Crossing it triggers **brownout**, not a stop: growth writes
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
mid-write — with recovery and back-fill verified (ADR 0044 P2).

**Why the mark is an aggregate, and what closes the gap.** One store can consume the whole
budget and brown out the others. That is enforced at the aggregate on purpose, not for lack
of effort: the resource being protected is *one filesystem*, and two of the four stores have
no client write to refuse — `replicas.redb` grows from other nodes' already-committed
appends and `lease.redb` from consensus itself, so "refuse the writes to the over-share
store" is undefinable for half the enumeration, and a follower refusing committed entries
would be thinning its group's replica count (the `min_replicas` floor's job), not enforcing
a watermark. What *was* missing is visibility, and that is now in the broker: when any
single store passes **70% of the aggregate mark** it is named once in a WARN (clearing
below 60%), before brownout rather than after. Alert on it too:
`max by (store) (mqttd_store_bytes) / scalar(mqttd_store_max_bytes) > 0.6 and on() mqttd_store_max_bytes > 0`
— `scalar()` is required because the left side carries a `store` label and the mark does
not, so a bare vector-to-vector divide matches nothing and the rule is silently inert.
Selective refusal for
the two stores where it *is* definable (`sessions`, `retained`) remains ADR 0041 T9.

**A browned-out node still grows `replicas.redb`.** The refusal is decided at the session's
owner, so a node that merely *follows* a group keeps applying its peers' committed appends
while browned out — and those are full message payloads, not metadata. On a cluster node
(the default) the dominant store's growth is therefore not gated locally at all: headroom
below the mark must cover peer-driven growth for the whole detect-and-recover window, not
just this node's own clients.

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

**Measured, not just claimed** (issue #244) — on **five broker processes sharing one
8-core host**, which is the dominant caveat and is why the numbers are ratios rather than
capacities: with two of five nodes' peer bus degraded so that one placement group's appends
stall completely, publishes for sessions in *healthy* groups on the same node **kept
flowing** while the degraded group's own publishes stopped (0.00–0.01× throughput), and the
hub loop's own publish dispatch p99 never left ~0.2 ms.

The healthy class was **not** unaffected, and the full picture matters more than the
flattering half of it: its latency improved (p50 0.31–0.47×, p95 0.35–0.50× across 4 runs)
*because* its throughput fell — **0.41–0.77×** in the same four runs. Fewer messages in
flight is why each one went faster. So the honest claim is isolation of *failure*, not of
*capacity*: a degraded group cannot stop or stall a healthy group's publishes, but on a
shared host it does take a share of the throughput with it. Whether that share survives on
separate hosts is exactly what the unrun multi-host lane would answer.

Two further caveats belong beside those: the *tail beyond p95* could not be attributed on
the measuring host (every phase, baseline included, carried unrelated 10–20 s stalls), and
a client whose **own** group is degraded does pay the 5 s RPC bound on CONNECT — 24–29 s at
p99 under the fault — so "connects are unaffected" holds for clients in healthy groups, not
universally. Method, limits and the commands:
[benchmarks/DURABLE-PATH.md](benchmarks/DURABLE-PATH.md).

**And what the durable write costs per message** (same source, single-host and dev-grade):
an acked QoS 1 publish to a persistent subscriber took ~28 ms at p50 against ~0.03 ms for
the same publish to a clean session, and the node's durable append rate was pinned by the
host's **per-volume** disk barrier (~215–240 flushes/s, shared by every store on the machine)
rather than by CPU — which is the number to re-measure on your own hardware before sizing
a write rate, because it is the one that decides it.

**The broker now measures that number for you** (ADR 0076): a durable node
probes its data-dir volume shortly after start and publishes the result —
`store_barrier_floor` (single-writer fsync round trips per second) and
`store_barrier_floor_4stream` (the parallel-stream aggregate) on `/metrics`,
and the same pair with the live group-commit shape under `store` on
`/statusz`. Sizing rule of thumb: sustainable durable msg/s ≈ barrier floor ×
the writer's mean batch (`durable_writer_ops` / `durable_writer_batches`),
divided by the replication factor's share on multi-node clusters. If
`rate(durable_writer_commit_micros)/rate(durable_writer_batches)` drifts well
above the boot figure, the volume has degraded (a noisy neighbor, throttling)
— alert on it.

**Why more parallel-stream headroom does not mean more throughput.** A volume
that serves 3.7× the barriers at 8 streams looks like 3.7× of headroom going
unused by a single-file store. It is not. Group commit (ADR 0071/0075) already
turns concurrency into **batch depth**: throughput is `barrier rate × mean
batch`. Splitting the store into K files divides that batch by K to buy `P(K)`
more barriers, so it scales `P(K)/K` — a loss unless the device serves K truly
independent queues. Measured: 0.85× at K=2, 0.58× at K=4 (ADR 0076 T2). The
store is therefore one file; the broker measures your volume's `P(K)` curve at
boot and only raises `store_reshard_advice` if yours is the rare device where
sharding would pay. `MQTTD_STORE_SHARDS` exists for that case and is
experimental — bring your own A/B.

The lever that *does* raise durable throughput is the one that makes batches
deeper, not narrower: more concurrent in-flight publishes per node.

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
memory_max_bytes   = 2.4 GiB  → 80% of a 3 GiB container limit; the other 20% is the
                                overshoot allowance (see the formula above)
watermark_poll_secs= 10       → both watchers; 1 s once within 10% of either mark
```

The full commented preset: [examples/bounded-node.toml](examples/bounded-node.toml).
Verify your numbers under load with the bench harness (`bench/run.sh`) and watch
`store_bytes`, `quota_rejections_total`, and RSS.

## What cannot be bounded today

Printed here so nobody discovers it in production:

| Axis | What bounds it today | Tracked by |
|---|---|---|
| Total process RSS **hard cap** | Watermark + brownout (`MQTTD_MEMORY_MAX_BYTES`, above), overshooting by up to `MQTTD_WATERMARK_POLL x allocation rate`; the container/cgroup limit is the ceiling. No in-process ceiling **by design** — allocation denial needs `unsafe` the workspace forbids, EMQX-style connection kills would not bound the dominant term, and both destroy standing state | — by design |
| Offline queue **bytes** | Message count only (`MQTTD_MAX_QUEUED_MESSAGES`) | 0041-T6 |
| Bridge-spool **bytes** | Message count only (default 10 000) | 0041-T7 |
| Per-store disk share | Aggregate watermark, plus a WARN naming any store over 70% of it and the `store_bytes{store}` gauge; no per-store *refusal* — and `replicas`/`lease` have no client write to refuse | 0041-T9 |
| Per-connection **write** buffering: the outbound channel's **packet** count | Still the hard-coded 10 000 packets (its *bytes* are `MQTTD_MAX_OUTBOUND_BYTES` since issue #241) | 0041-T10 |
| Refusing a **publisher** instead of shedding acked entries at the backlog byte bound | Nothing — the bound sheds, and the publisher is not told | 0041-T15 |

**The per-subscriber write path, corrected (issue #241).** The earlier "~10 GiB per
connection that no setting can lower" counted **one** of three per-subscriber in-memory
structures. Unset, a stalled subscriber can hold the in-flight window (**65 535** entries —
every v3.1.1 client and any v5 client that sends no Receive Maximum gets `u16::MAX`), the
flow-control backlog (10 000), and the outbound channel (10 000 packets): at the 1 MiB
default packet size that is `(65 535 + 10 000 + 10 000) x max_packet_size` ≈ **84 GiB**, not
10 GiB. All three are lowerable now — `MQTTD_MAX_INFLIGHT_MESSAGES` (a gate on the wire window — it drops
nothing itself, but the surplus it holds back waits in the drop-oldest backlog, so it is not
loss-free),
`MQTTD_MAX_BACKLOG_MESSAGES` / `MQTTD_MAX_BACKLOG_BYTES` (drop-oldest), and
`MQTTD_MAX_OUTBOUND_BYTES` (`QoS` 0 shed) — and capping `MQTTD_MAX_PACKET_SIZE` still
multiplies through every term.

Two things to know before setting the byte bound low. It sheds messages that were already
**stored and acked**, without telling the publisher — the same ack-and-drop arm the
offline-queue cap has, reached earlier. And each eviction runs one on-loop store truncate,
so a bound below `max_packet_size` turns a rare path into a per-publish one and shows up in
`mqttd_hub_dispatch_seconds`'s `publish` class (startup warns when you configure that;
routing the truncate through the session's append lane is the ADR 0061 residual that fixes
it).

All of these are recorded losses in [COMPARISON.md](COMPARISON.md).
