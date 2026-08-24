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
