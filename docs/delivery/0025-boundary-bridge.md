---
adr: "0025"
title: Boundary MQTT bridge to brokers in other security zones
adr_status: Accepted
tasks:
  - id: 0025-T1
    title: New mqtt-bridge crate and binary skeleton (MQTT client built on mqtt-codec/mqtt-net)
    status: done
    date: 2026-06-25
    evidence: "crates/mqtt-bridge: MqttClient (connect over plain TCP / TLS-mTLS via mqtt-net, CONNECT/SUBSCRIBE/PUBLISH/PUBACK/PING/DISCONNECT, next_event loop) on mqtt-codec/mqtt-net; binary skeleton. Test the_client_connects_subscribes_and_round_trips_a_publish (against an in-process mqttd; the hop-count User Property survives the broker hop now that ADR 0030 forwards it) + connect_refused_surfaces_as_an_error."
  - id: 0025-T2
    title: Config model and validation (upstreams, per-rule direction/filter/remap/qos, deny-by-default)
    status: done
    date: 2026-06-25
    evidence: "config.rs: BridgeConfig (TOML, deny_unknown_fields) — local endpoint + N upstreams, each with direction/filter/remap/qos rules; validate() rejects a zero hop limit, malformed filters, duplicate upstream names, an mTLS half-identity, and both password sources. 9 tests."
  - id: 0025-T3
    title: Client engine (connect cluster + each upstream over TLS/mTLS, subscribe/publish, reconnect with backoff)
    status: done
    date: 2026-06-25
    evidence: "engine.rs Bridge::start — one supervised connection per side (connect, subscribe per direction, MqttClient::run pump, reconnect with bounded backoff), a central router, clean shutdown. client.rs gained Command + run (concurrent read/write select). Integration test a_one_way_out_rule_forwards_to_the_upstream_and_never_leaks_back over two in-process brokers."
  - id: 0025-T4
    title: Directional forwarding and topic remap; a one-way rule never opens the reverse path in code
    status: done
    date: 2026-06-25
    evidence: "forward::plan_forwards — local-origin only forwards out (out/both rules), upstream-origin only forwards in (in/both); a one-way rule cannot produce a reverse forward, AND local/upstream_subscriptions never subscribe the closed side. apply_remap (strip+prefix). Tests a_one_way_out/in_rule_never_forwards_*, subscriptions_follow_direction + the live no-leak integration test."
  - id: 0025-T5
    title: Loop prevention via fss-bridge-hop-count user property + configurable hop-count-limit (plus remap discipline)
    status: done
    date: 2026-06-25
    evidence: "read_hop_count/set_hop_count (preserve other user properties, drop connection-scoped props); plan_forwards drops a message at hop_count_limit; the router stamps hop+1 on each forward. Unblocked by ADR 0030 (user properties survive the broker hop). Tests the_hop_limit_drops_a_message_at_the_limit, hop_count_reads_default_zero_and_increments_preserving_other_props + the live integration test (hop-count=1 observed)."
  - id: 0025-T6
    title: HA via cluster-side shared subscriptions and a persistent session (dedup across instances)
    status: done
    date: 2026-06-25
    evidence: "local_subscriptions wrap each filter as $share/<group>/<filter> (config share_group, default fss-bridge); the local side connects persistent (clean_start=false). Test two_bridge_instances_do_not_duplicate_forwarding (two instances, one shared group → each message forwarded at most once) + local_subscriptions_wrap_in_the_share_group_for_ha."
  - id: 0025-T7
    title: Bounded disk-backed store-and-forward spool for transient outages, replayed on reconnect
    status: done
    date: 2026-06-25
    evidence: "spool.rs Spool — bounded FIFO (drop-oldest past spool.max_messages, default 10000), disk-backed via redb when spool.dir is set (survives a bridge restart), else in-memory; a length-prefixed codec preserves topic/payload/qos/user-properties (incl. the hop count). The router spools a forward whose destination is disconnected (a connected AtomicBool per side); the supervisor replays the spool oldest-first on reconnect (run loop assigns packet ids). Tests: spool unit tests (bounded drop-oldest, disk round-trip + reopen persistence, cap) + integration messages_spooled_while_an_upstream_is_down_replay_on_reconnect."
  - id: 0025-T8
    title: Per-side least-privilege credentials (publish-only/subscribe-only) and per-upstream mTLS identity + audit
    status: done
    date: 2026-06-25
    evidence: "Per-side username/password (+ password_file) and per-upstream mTLS identity (Tls ca/cert/key) carried into each ConnectOptions (connect_options); every forward writes an audit record (bridge::audit target: upstream, direction, src, dst) via BridgeMetrics::forwarded. Least-privilege (publish-only/subscribe-only) is a broker-side ACL on the bridge's account — an operator/deployment control documented for T11; the bridge supplies the distinct identity."
  - id: 0025-T9
    title: Bridge observability (forwarded/dropped per upstream+direction, lag, reconnects) via mqtt-observability + OTLP
    status: done
    date: 2026-06-25
    evidence: "metrics.rs BridgeMetrics: forwarded (out/in), dropped (hop-limit), reconnects; render() emits Prometheus text (ADR 0020 format); Bridge::metrics() exposes the handle. Wired into the router + supervisors. Test counters_increment_and_render + the engine test asserts forwarded_out after a real forward. (OTLP export reuses the broker's mqtt-observability pattern when a metrics bind is added — a follow-up; Prometheus text + audit log are in place.)"
  - id: 0025-T10
    title: Adversarial tests (one-way never leaks reverse; loop prevention; ACL deny; reconnect/spool; multi-upstream; shared-sub dedup)
    status: done
    date: 2026-06-25
    evidence: "tests/engine.rs over real in-process brokers: a_one_way_out_rule_forwards_to_the_upstream_and_never_leaks_back (one-way never leaks), a_no_remap_both_rule_loop_is_bounded_by_the_hop_limit (loop self-terminates at the hop limit), a_local_message_fans_out_to_multiple_upstreams (multi-upstream), two_bridge_instances_do_not_duplicate_forwarding (shared-sub dedup) — plus the exhaustive pure forward:: tests. ACL-deny is a broker-side control (the bridge holds a least-privilege account and simply gets no delivery / a denied publish, ADR 0004) not separately simulated here; reconnect is the T3 supervisor; spool replay is T7."
  - id: 0025-T11
    title: Demo + docs — bridge the cluster to a second isolated broker, one-way and bidirectional
    status: planned
---

# Delivery — ADR 0025: Boundary MQTT bridge

Decision: [docs/adr/0025-boundary-bridge.md](../adr/0025-boundary-bridge.md).

A standalone `mqtt-bridge` component — an MQTT client to both the local cluster and one or
more external brokers — that forwards configured topics across a security-zone boundary,
with per-rule direction (and **enforced** unidirectional flow as the headline security
control). Accepted and under construction; every phase lands test-first, with the
one-way-never-leaks-reverse property as the central adversarial test.

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
| 0025-T1 | ✅ done | 2026-06-25 | "crates/mqtt-bridge: MqttClient (connect over plain TCP / TLS-mTLS via mqtt-net, CONNECT/SUBSCRIBE/PUBLISH/PUBACK/PING/DISCONNECT, next_event loop) on mqtt-codec/mqtt-net; binary skeleton. Test the_client_connects_subscribes_and_round_trips_a_publish (against an in-process mqttd; the hop-count User Property survives the broker hop now that ADR 0030 forwards it) + connect_refused_surfaces_as_an_error." |
| 0025-T2 | ✅ done | 2026-06-25 | "config.rs: BridgeConfig (TOML, deny_unknown_fields) — local endpoint + N upstreams, each with direction/filter/remap/qos rules; validate() rejects a zero hop limit, malformed filters, duplicate upstream names, an mTLS half-identity, and both password sources. 9 tests." |
| 0025-T3 | ✅ done | 2026-06-25 | "engine.rs Bridge::start — one supervised connection per side (connect, subscribe per direction, MqttClient::run pump, reconnect with bounded backoff), a central router, clean shutdown. client.rs gained Command + run (concurrent read/write select). Integration test a_one_way_out_rule_forwards_to_the_upstream_and_never_leaks_back over two in-process brokers." |
| 0025-T4 | ✅ done | 2026-06-25 | "forward::plan_forwards — local-origin only forwards out (out/both rules), upstream-origin only forwards in (in/both); a one-way rule cannot produce a reverse forward, AND local/upstream_subscriptions never subscribe the closed side. apply_remap (strip+prefix). Tests a_one_way_out/in_rule_never_forwards_*, subscriptions_follow_direction + the live no-leak integration test." |
| 0025-T5 | ✅ done | 2026-06-25 | "read_hop_count/set_hop_count (preserve other user properties, drop connection-scoped props); plan_forwards drops a message at hop_count_limit; the router stamps hop+1 on each forward. Unblocked by ADR 0030 (user properties survive the broker hop). Tests the_hop_limit_drops_a_message_at_the_limit, hop_count_reads_default_zero_and_increments_preserving_other_props + the live integration test (hop-count=1 observed)." |
| 0025-T6 | ✅ done | 2026-06-25 | "local_subscriptions wrap each filter as $share/<group>/<filter> (config share_group, default fss-bridge); the local side connects persistent (clean_start=false). Test two_bridge_instances_do_not_duplicate_forwarding (two instances, one shared group → each message forwarded at most once) + local_subscriptions_wrap_in_the_share_group_for_ha." |
| 0025-T7 | ✅ done | 2026-06-25 | "spool.rs Spool — bounded FIFO (drop-oldest past spool.max_messages, default 10000), disk-backed via redb when spool.dir is set (survives a bridge restart), else in-memory; a length-prefixed codec preserves topic/payload/qos/user-properties (incl. the hop count). The router spools a forward whose destination is disconnected (a connected AtomicBool per side); the supervisor replays the spool oldest-first on reconnect (run loop assigns packet ids). Tests: spool unit tests (bounded drop-oldest, disk round-trip + reopen persistence, cap) + integration messages_spooled_while_an_upstream_is_down_replay_on_reconnect." |
| 0025-T8 | ✅ done | 2026-06-25 | "Per-side username/password (+ password_file) and per-upstream mTLS identity (Tls ca/cert/key) carried into each ConnectOptions (connect_options); every forward writes an audit record (bridge::audit target: upstream, direction, src, dst) via BridgeMetrics::forwarded. Least-privilege (publish-only/subscribe-only) is a broker-side ACL on the bridge's account — an operator/deployment control documented for T11; the bridge supplies the distinct identity." |
| 0025-T9 | ✅ done | 2026-06-25 | "metrics.rs BridgeMetrics: forwarded (out/in), dropped (hop-limit), reconnects; render() emits Prometheus text (ADR 0020 format); Bridge::metrics() exposes the handle. Wired into the router + supervisors. Test counters_increment_and_render + the engine test asserts forwarded_out after a real forward. (OTLP export reuses the broker's mqtt-observability pattern when a metrics bind is added — a follow-up; Prometheus text + audit log are in place.)" |
| 0025-T10 | ✅ done | 2026-06-25 | "tests/engine.rs over real in-process brokers: a_one_way_out_rule_forwards_to_the_upstream_and_never_leaks_back (one-way never leaks), a_no_remap_both_rule_loop_is_bounded_by_the_hop_limit (loop self-terminates at the hop limit), a_local_message_fans_out_to_multiple_upstreams (multi-upstream), two_bridge_instances_do_not_duplicate_forwarding (shared-sub dedup) — plus the exhaustive pure forward:: tests. ACL-deny is a broker-side control (the bridge holds a least-privilege account and simply gets no delivery / a denied publish, ADR 0004) not separately simulated here; reconnect is the T3 supervisor; spool replay is T7." |
| 0025-T11 | ⬜ planned | — |  |
<!-- /status-table:0025 -->

## Changelog

- **2026-06-25** — ADR ratified (Accepted) and T1 landed: the `mqtt-bridge` crate + a
  minimal `MqttClient` (TCP/TLS over `mqtt-codec`/`mqtt-net`), proven against an in-process
  broker. Building T1 surfaced that the broker dropped MQTT 5 User Properties on delivery —
  a conformance gap (MQTT-3.3.2-17) and a blocker for the hop-count loop-prevention (T5).
  Fixed first as [ADR 0030](../adr/0030-user-property-forwarding.md) (User Properties now
  forwarded end to end), which unblocks T5. Remaining tasks T2–T11 in progress.
- **2026-06-23** — ADR proposed and delivery doc opened; all tasks `planned` pending design
  review. The decision (separate component vs in-process plugin; enforced unidirectional
  flow; shared-subscription HA) is up for argument before any code is written.
- **2026-06-23** — Loop-prevention design (T5) refined per review: a `fss-bridge-hop-count`
  MQTT 5 user property incremented on each forward, dropped at a configurable
  `hop-count-limit`, bounds any multi-bridge cycle (replacing the simpler origin-marker
  backstop). 3.1.1 boundaries fall back to direction + remap, logged not silent.
