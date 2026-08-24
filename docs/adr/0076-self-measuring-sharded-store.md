# 0076. The self-measuring sharded store — the volume's capacity becomes the broker's business

Date: 2026-08-24
Status: Accepted

## Context

With ADR 0075 shipped, the durable path is store-bound again — and the store
runs ONE serialized fsync stream per node (`replicas.redb`) while the volume
underneath serves several. Measured on the v1.0.5 curve hosts
(`device_barrier_floor` / `store_append_floor`):

| streams (separate files) | aggregate barriers/s |
|---|---|
| 1 | 2,162 |
| 3 | 4,064 |
| 8 | 8,041 |

One redb store: 3,780 serial appends/s, 24,190/s at 32-way concurrency
(group commit); three stores on one device add ~1.3× even fully serial.
Meanwhile cross-host and cross-run variance is large and REAL (1,941–2,521
this campaign; 3.7× across hosts in v1.0.3) — any statically-tuned number
goes stale, and today an operator needs our bench rig to learn any of this.

## Decision

Three slices, measurement before adaptation, layout committed rather than
flapping (issue #403):

1. **Self-measurement, exposed.** At first boot (empty data dir) the broker
   probes its own volume — the rig's barrier sweep, ~100 ms — and records
   the result. Thereafter it measures passively: every group-commit is a
   barrier sample, so per-epoch (60 s) barrier latency, effective batch
   depth, and estimated stream headroom are maintained from live traffic
   and published via `/metrics` + `/statusz`. Drift (a noisy neighbor, a
   volume migration) becomes an alertable signal instead of a mystery
   regression.
2. **The store shards into K files, K calibrated once.** Placement groups
   map to shards by stable hash; each shard is its own redb database with
   its own ADR 0071 writer — per-shard FIFO preserves every ordering
   invariant, so the change is pure-performance by construction. K is
   chosen at first boot from the probe (bounded 1..=8, the measured knee),
   committed in the store's schema metadata (ADR 0038 gate; existing
   single-file stores read as K=1 and stay K=1 — no migration on upgrade),
   and never changes at runtime. When the epoch measurements say the
   committed K no longer fits the volume, the broker says so loudly — a
   reshard ADVISOR (metric + statusz + log), never a silent migration.
3. **Epoch-adaptive coalescing.** Each shard's writer gains a linger that is
   a FUNCTION OF THE MEASURED commit time (a fraction of one barrier,
   recomputed per epoch), engaged only while the writer is saturated
   (nonempty queue at commit end) — deeper batches on degraded volumes,
   zero added latency uncontended, no operator knob. A pin
   (`MQTTD_STORE_LINGER=0`) disables it loudly for tests and benches.

## Consequences

- The ceiling multiplies: the measured device does ~3.7× more barriers at
  8 streams, on top of ADR 0075's ~113 ops/barrier batching.
- Ops get the volume's truth from the broker itself — boot-time and live —
  instead of from a paid bench run.
- A layout change (K) is a schema-versioned, operator-initiated act with an
  advisor pointing at it; upgrades never reshard implicitly.

## Alternatives considered

- **Dynamic K at runtime:** resharding moves committed data exactly when
  the disk is struggling, and is rollback-hostile; rejected in favor of
  calibrate-once + advise.
- **One store, more concurrency:** redb serializes writers per database;
  the 32-way figure is group-commit depth, not stream parallelism — the
  device's extra streams are only reachable with separate files.
- **Tuning by documentation (SIZING.md tables):** goes stale with the
  volume; the broker measuring itself is the only version that stays true.

## Amendment (2026-08-24) — T2's sharding hypothesis is FALSIFIED

Decision 2 above is wrong, and the measurement it asked for is what shows it.
The store stays **one file by default**; K > 1 survives only as an explicit,
loudly-warned operator pin.

**The arithmetic.** ADR 0075's group-commit writer converts in-flight work
into batch **depth**: throughput is `D × barriers/s`, where `D` is how many
ops coalesce into one barrier (measured at ~113 on the campaign hosts).
Splitting the store into K files gives each shard depth `D/K`, while the
device multiplies its barrier rate by only `P(K)` — its parallel-stream gain.
So:

```text
    sharded / single  =  P(K) / K
```

Sharding needs `P(K) ≈ K`: a device serving K *genuinely independent* queues.
The context table above reads `P(4) ≈ 1.9`, `P(8) ≈ 3.7` — which this ADR
mistook for headroom to exploit. It is headroom that costs more than it pays.

**The measurement** (local, release, the same 48×8×48 shape ADR 0075 used, one
node, `MQTTD_STORE_SHARDS` pinned):

| K | acked msg/s | vs K=1 | append mean | predicted `P(K)/K` |
|---|---|---|---|---|
| 1 | 25,270 / 24,884 | — | 11.4 / 11.8 ms | 1.00 |
| 2 | 21,344 | 0.85× | 16.6 ms | 0.85 |
| 4 | 14,531 | 0.58× | 24.9 ms | 0.58 |

Prediction and measurement agree to within run-to-run noise, so this is a
mechanism, not a bad afternoon on one disk.

**What ships instead:**

1. **K = 1 by default, forever, on every store.** No first-boot calibration —
   there is nothing to calibrate toward.
2. **The mechanism is retained** behind `MQTTD_STORE_SHARDS=<2..8>`, honored
   only for a **fresh** data dir and committed to that store's schema for its
   life. It warns at boot that it is experimental and measured slower. It
   exists so the finding stays falsifiable on hardware we have not met — a
   device with independent per-file queues would show `P(K) ≈ K`.
3. **The self-measurement is kept and sharpened** (T1's whole point): the boot
   probe now also measures the volume's parallel-barrier **curve** at 1/2/4/8
   streams, publishes it, and applies the `P(K)/K` rule. `store_shards`
   reports the committed layout; `store_reshard_advice` stays **0** unless the
   device really would pay — a gauge whose silence is the finding.
4. **The commit moved out of the state lock** regardless of K. The writer now
   decides under the lock, fsyncs with it **released**, and applies under it
   again — safe because a shard's groups have exactly one writer. At K=1 that
   is a pure latency win for every reader (recovery reads, `/statusz`, the
   catch-up sweep) that used to queue behind an 11 ms fsync.

The ADR's own rule — *measurement before adaptation* — is what produced this,
and the honest outcome of measuring first is sometimes not adapting. The
store's ceiling is still the store's ceiling; ADR 0076 T3 (adaptive
coalescing, which raises `D` rather than dividing it) is now the live lead.
