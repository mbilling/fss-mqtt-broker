---
adr: "0025"
title: Boundary MQTT bridge to brokers in other security zones
adr_status: Proposed
tasks:
  - id: 0025-T1
    title: New mqtt-bridge crate and binary skeleton (MQTT client built on mqtt-codec/mqtt-net)
    status: planned
  - id: 0025-T2
    title: Config model and validation (upstreams, per-rule direction/filter/remap/qos, deny-by-default)
    status: planned
  - id: 0025-T3
    title: Client engine (connect cluster + each upstream over TLS/mTLS, subscribe/publish, reconnect with backoff)
    status: planned
  - id: 0025-T4
    title: Directional forwarding and topic remap; a one-way rule never opens the reverse path in code
    status: planned
  - id: 0025-T5
    title: Loop prevention via fss-bridge-hop-count user property + configurable hop-count-limit (plus remap discipline)
    status: planned
  - id: 0025-T6
    title: HA via cluster-side shared subscriptions and a persistent session (dedup across instances)
    status: planned
  - id: 0025-T7
    title: Bounded disk-backed store-and-forward spool for transient outages, replayed on reconnect
    status: planned
  - id: 0025-T8
    title: Per-side least-privilege credentials (publish-only/subscribe-only) and per-upstream mTLS identity + audit
    status: planned
  - id: 0025-T9
    title: Bridge observability (forwarded/dropped per upstream+direction, lag, reconnects) via mqtt-observability + OTLP
    status: planned
  - id: 0025-T10
    title: Adversarial tests (one-way never leaks reverse; loop prevention; ACL deny; reconnect/spool; multi-upstream; shared-sub dedup)
    status: planned
  - id: 0025-T11
    title: Demo + docs — bridge the cluster to a second isolated broker, one-way and bidirectional
    status: planned
---

# Delivery — ADR 0025: Boundary MQTT bridge

Decision: [docs/adr/0025-boundary-bridge.md](../adr/0025-boundary-bridge.md).

A standalone `mqtt-bridge` component — an MQTT client to both the local cluster and one or
more external brokers — that forwards configured topics across a security-zone boundary,
with per-rule direction (and **enforced** unidirectional flow as the headline security
control). Proposed: the decision is up for review before any code; every phase lands
test-first, with the one-way-never-leaks-reverse property as the central adversarial test.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0025-T1** Skeleton | A `mqtt-bridge` crate + binary that builds and connects to one broker as an MQTT client, reusing `mqtt-codec`/`mqtt-net` (TLS); no forwarding yet. |
| **0025-T2** Config | TOML config: a local-cluster connection + N upstreams (URL, TLS/mTLS, credentials), each with mapping rules (`direction` out/in/both, `filter`, `remap` strip/prefix, `qos`). Validation rejects ambiguous/loop-prone rules; forwarding is deny-by-default. |
| **0025-T3** Engine | Connect the cluster + every upstream concurrently over TLS/mTLS; subscribe and publish per the rules; reconnect with bounded backoff; clean shutdown. |
| **0025-T4** Direction + remap | Each rule forwards only in its configured direction with its topic remap applied; an `out`/`in` rule provably **never opens** the reverse subscription. |
| **0025-T5** Loop prevention | Every forward increments an MQTT 5 `fss-bridge-hop-count` user property; a message at the configured `hop-count-limit` is dropped (reason `hop-limit`), so any multi-bridge cycle self-terminates in bounded hops. Direction + remap still prevent the immediate echo; the 3.1.1 fallback (no user properties) is logged, not silent. |
| **0025-T6** HA | ≥2 bridge instances subscribe on the cluster side via a shared subscription with a persistent session: the stream is load-balanced, deduplicated, and survives a single instance restart. |
| **0025-T7** Store-and-forward | A bounded, disk-backed spool holds messages for a momentarily-unreachable side and replays them on reconnect, dropping oldest past the cap (never unbounded). |
| **0025-T8** Least privilege | Documented + enforced per-side credentials (publish-only / subscribe-only on allowed topics) and a distinct mTLS identity per upstream; an audit record of what crossed, in which direction. |
| **0025-T9** Observability | Metrics for forwarded/dropped per upstream+direction, queue lag, and reconnects, exported to the shared registry (Prometheus + OTLP, ADR 0020). |
| **0025-T10** Adversarial tests | Over two real brokers (a second `mqttd` as the "external" side): a one-way rule never leaks the reverse direction; loops are impossible; an ACL-denied topic does not cross; a reconnect replays the spool without loss/dup beyond the QoS contract; multiple upstreams and shared-sub dedup hold. |
| **0025-T11** Demo + docs | Extend `demo/` with a second, isolated broker and a bridge between it and the cluster — one unidirectional mapping and one bidirectional — plus operator docs. |

## Progress

<!-- status-table:0025 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0025-T1 | ⬜ planned | — |  |
| 0025-T2 | ⬜ planned | — |  |
| 0025-T3 | ⬜ planned | — |  |
| 0025-T4 | ⬜ planned | — |  |
| 0025-T5 | ⬜ planned | — |  |
| 0025-T6 | ⬜ planned | — |  |
| 0025-T7 | ⬜ planned | — |  |
| 0025-T8 | ⬜ planned | — |  |
| 0025-T9 | ⬜ planned | — |  |
| 0025-T10 | ⬜ planned | — |  |
| 0025-T11 | ⬜ planned | — |  |
<!-- /status-table:0025 -->

## Changelog

- **2026-06-23** — ADR proposed and delivery doc opened; all tasks `planned` pending design
  review. The decision (separate component vs in-process plugin; enforced unidirectional
  flow; shared-subscription HA) is up for argument before any code is written.
- **2026-06-23** — Loop-prevention design (T5) refined per review: a `fss-bridge-hop-count`
  MQTT 5 user property incremented on each forward, dropped at a configurable
  `hop-count-limit`, bounds any multi-bridge cycle (replacing the simpler origin-marker
  backstop). 3.1.1 boundaries fall back to direction + remap, logged not silent.
