# Sizing a node — the operational unknowns

**Who this is for:** an operator putting mqttd on a machine with a known memory and
disk budget, deciding which limits to set before exposing it to real load.

**The honest headline:** mqttd's defaults are *safe against attackers but not against
success*. Authentication is deny-by-default, packets are capped at 1 MiB, and every
quota refuses loudly at the edge (ADR 0041 — reason codes and backpressure, never
silent drops). But connections, sessions, retained topics, and disk are all
**uncapped until you cap them**, the per-session offline queue defaults to 100 000
messages, and there is **no total-memory knob** — memory is bounded by arithmetic,
not by a setting. This page is that arithmetic.

A ready-made preset with this page's numbers: [examples/bounded-node.toml](examples/bounded-node.toml).

## The minimum decisions

Eight numbers bound a node. Everything else has a safe default.

| Decision | Knob (env / TOML `[limits]` unless noted) | Ships as |
|---|---|---|
| Where state lives | `MQTTD_DATA_DIR` (`[node]`) | unset = **everything in memory** |
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

There is no `memory_limit`. Size RSS from the caps you set:

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
| Retained mutations queued during heal (ADR 0037 §5) | 1 024, drop-oldest, counted |
| Peer-link read buffer | 32 MiB per peer |
| Peer frame / raft RPC / SWIM datagram | 16 MiB / 4 MiB / 64 KiB |

Run the broker under a container memory limit sized to the formula plus slack. The
durable design makes an OOM-kill recoverable (acked state is on disk/quorum and
crash-recovery is continuously tested, ADR 0044), but recovery is not free — the
point of the arithmetic is to not get there. A broker-side RSS watermark with
brownout semantics is accepted work (ADR 0041 amendment T8).

## Disk: the formula

```
disk ≥ store_max_bytes + headroom (WAL/compaction slack + logs; ~20% is a sane start)
```

`MQTTD_STORE_MAX_BYTES` is **one aggregate high-water mark** across the four stores
(`sessions`, `retained`, `replicas`, `lease` — polled every 10 s, exported as
`store_bytes{store}`). Crossing it triggers **brownout**, not a stop: growth writes
are refused (new retained topics, new sessions, offline enqueues — counted in
`quota_rejections_total{kind="brownout"}`), while acks, reads, deletes, and expiry
continue, and dropping below the mark restores writes. It is a high-water mark, not a
hard wall — hence the headroom.

Past the headroom, actual disk-full **fails closed**: a write that cannot be made
durable withholds the publisher's ack (the publisher retries; nothing is silently
lost). This is crash-tested in the harshest form — the kernel killing the process
mid-write — with recovery and back-fill verified (ADR 0044 P2). The watermark is an
**aggregate** over all four stores: there is no per-store quota yet, so one store can
consume the whole budget and brown out the others (ADR 0041 amendment T9).

If `MQTTD_DATA_DIR` is unset, none of this applies — all state is in memory and the
memory formula is the only budget.

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
| Total process RSS | Nothing — container limits + this page | 0041-T8 |
| Offline queue **bytes** | Message count only (`MQTTD_MAX_QUEUED_MESSAGES`) | 0041-T6 |
| Bridge-spool **bytes** | Message count only (default 10 000) | 0041-T7 |
| Per-store disk share | Aggregate watermark only | 0041-T9 |
| Per-connection **write** buffering | Two hard-coded count caps, neither in bytes and neither configurable: `MAX_BACKLOG` (10 000 messages, QoS 1/2, drop-oldest) and the outbound queue (10 000 packets, QoS 0, shed and counted) | 0041-T10 |

The last row is the sharpest edge: a subscriber that stops reading can hold up to
10 000 messages, and at the 1 MiB default packet size that is ~10 GiB of headroom per
connection that no setting can lower. Cap `MQTTD_MAX_PACKET_SIZE` to bound it in
practice — the product of the two is the real worst case.

All five are recorded losses in [COMPARISON.md](COMPARISON.md).
