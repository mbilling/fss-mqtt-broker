# Performance baseline

Recorded hot-path numbers ([ADR 0044](../adr/0044-release-readiness-assurance.md)
P6). These are **micro-benchmark** numbers — the CPU cost of the code on the
hottest paths, isolated from I/O — measured with
[criterion](https://github.com/bheisler/criterion.rs). They are honest, they are
reproducible, and they are what a code change is checked against.

They are **not** an end-to-end throughput claim: a delivered message also pays
network, fsync, and scheduling costs that dominate the microseconds below. What
these numbers guarantee is that the broker's own CPU work is not the bottleneck
and does not silently regress.

## Reference machine

- 4× Intel Xeon @ 2.80GHz, Linux, `--release` (opt-level 3, thin LTO)
- Recorded 2026-07-17. Absolute values are machine-specific; the shape and
  order of magnitude are what matter, and the regression **gate** (below) is
  relative, not tied to these exact figures.

## Codec (`cargo bench -p mqtt-codec`)

The MQTT wire codec — on the delivery path for every message, encode on the way
out and decode on the way in.

| Operation | Payload | Time (median) | Throughput |
|---|---|---|---|
| PUBLISH encode | 16 B | ~99 ns | ~480 MiB/s |
| PUBLISH encode | 256 B | ~271 ns | ~1.0 GiB/s |
| PUBLISH encode | 4 KiB | ~488 ns | ~7.9 GiB/s |
| PUBLISH decode | 16 B | ~193 ns | ~247 MiB/s |
| PUBLISH decode | 256 B | ~188 ns | ~1.4 GiB/s |
| PUBLISH decode | 4 KiB | ~185 ns | ~21 GiB/s |
| CONNECT decode | — | ~330 ns | — |
| SUBSCRIBE decode (2 filters) | — | ~285 ns | — |

A 256-byte PUBLISH round-trips (encode + decode) in ~460 ns — the codec alone
sustains on the order of a couple of million messages per second per core.

## Durable plane (`cargo bench -p mqtt-cluster`)

The cluster hot paths, in-memory (no fsync — the disk cost is the hardware's;
this isolates the CPU cost a code change can regress).

| Operation | Size | Time (median) | Throughput |
|---|---|---|---|
| replica apply | 64 B record | ~290 ns | ~3.4 Melem/s |
| replica apply | 1 KiB record | ~367 ns | ~2.7 Melem/s |
| peer frame encode | Replicate/256 B | ~439 ns | ~630 MiB/s |
| peer frame decode | Replicate/256 B | ~357 ns | ~772 MiB/s |

Re-baselined 2026-08-04 under the postcard codec (ADR 0052; previously bincode:
encode ~284 ns / decode ~418 ns). Encode pays ~150 ns for varint packing; decode
got faster. Both remain far below the per-message budget of the delivery path.

## Bridge (`cargo bench -p mqtt-bridge`)

The boundary bridge's per-message paths (ADR 0060 T7). Recorded as the **baseline for ADR
0060 T8** — the pending-ack model adds an obligation insert/lookup beside the routing
decision, and [ADR 0060 §5](../adr/0060-bridge-durability-and-ack-contract.md) claims that
costs latency, not throughput. That claim gets a before/after number rather than a paragraph.

> **Different machine.** Unlike the sections above, these were captured on a development
> machine (Apple Silicon, `--release`), not the Xeon reference box, so do **not** compare them
> across sections. What they establish is this suite's own shape and order of magnitude; the
> nightly `bench` job prints reference-machine numbers.

| Operation | Case | Time (median) |
|---|---|---|
| route (`plan_forwards`) | matching `out` rule + remap | ~170 ns |
| route (`plan_forwards`) | no rule matches (deny-by-default) | ~28 ns |
| stamp hop count | publisher sent 0 user properties | ~110 ns |
| stamp hop count | publisher sent 4 user properties | ~597 ns |
| partitioned ownership (`owns`) | 4 instances | ~0.37 ns |
| spool push (mem) | below the cap | ~8.9 ns |
| spool push (mem) | at the cap, `Refuse` (QoS≥1 default) | ~9.0 ns |
| spool push (mem) | at the cap, `DropOldest` (QoS 0) | ~214 ns |

Two things worth reading off this table:

- **Partitioned HA is free.** `owns` is a sub-nanosecond FNV hash, so making it the default
  (ADR 0059) costs nothing per message — the correctness win had no throughput price.
- **Refusing is ~24× cheaper than shedding.** At the cap, `Refuse` returns without touching
  the queue (~9 ns) while `DropOldest` evicts and audits the shed message (~214 ns). The
  safer default (ADR 0060 T2/T5 — never drop a message already acked to the source) is also
  the faster one.

The most expensive per-message step is rebuilding the property set when the publisher sent
several user properties (~597 ns at four) — a known allocation-per-property cost, and the
obvious optimisation target if the bridge ever needs one. It is not on the critical path for
T8.

## The regression gate

Two layers watch these numbers:

1. **Per-PR floor** (`cargo test -p mqtt-codec --test perf_gate`): a codec
   round-trip throughput assertion that runs on every PR in the unoptimized
   test profile. Its floor is set ~4× below the sustained rate, so machine
   variance never trips it but a gross regression (an allocation in the hot
   loop, an accidental O(n²)) fails the build immediately. It is a floor, not a
   target.

2. **Nightly benches** (the `bench` job): the full criterion suites run in the
   nightly tier, printing current numbers for comparison against this document.
   A meaningful shift is investigated and this baseline is updated with the
   commit that moved it.

## Reproducing

```sh
cargo bench -p mqtt-codec        # codec hot path
cargo bench -p mqtt-cluster      # durable-plane hot path
cargo bench -p mqtt-bridge       # bridge per-message hot paths
cargo test  -p mqtt-codec --test perf_gate   # the per-PR floor
```
