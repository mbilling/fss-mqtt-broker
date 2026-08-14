---
adr: "0048"
title: "Comparative performance benchmarking (published, reproducible, honest)"
adr_status: Accepted
tasks:
  - id: 0048-T1
    title: Containerized load harness — emqtt-bench + docker-compose that stands up each broker (ours, Mosquitto, EMQX) from its published image with documented reasonable config; same hardware, pinned versions, security posture held constant and disclosed
    status: done
    date: 2026-07-23
    evidence: "bench/: compose profiles run ONE broker at a time (mqttd built from source; Mosquitto 2.0.20; EMQX 5.8.6 — pinned), driven by emqtt-bench 0.6.3 (EMQX's own tool, ADR §3). run.sh executes identical scenarios per broker — connection-rate (timed window; emqtt_bench conn holds connections and never exits, learned in smoke), sustained pub/sub at QoS 0/1/2 (N pubs → N subs, 256 B), RSS snapshot — capturing raw logs + env.txt (versions/params/host, dev-grade label) per run; results/ is gitignored. Posture held constant and disclosed: plaintext/anonymous/in-memory on all three (mqttd explicitly opts out of durable-by-default; TLS posture is T2). Smoke-verified end-to-end on all three brokers: 100/100 connects, publishes complete at every QoS, exit 0."
  - id: 0048-T2
    title: The selection metrics — sustained throughput (QoS 0/1/2), end-to-end latency p50/p99/p999, memory per idle connection at scale, connection-establishment rate (mTLS included); full distributions, never a single number
    status: done
    date: 2026-07-24
    evidence: "bench/run.sh scenarios per broker in TWO disclosed postures (plaintext 1883, TLS+required-client-certs 8883 from bench/tls/gen-certs.sh — clientAuth EKU, the interop lesson): connection-establishment rate; memory per idle connection (broker RSS snapshotted before/after the conn ramp, settle-delay first); sustained pub/sub at QoS 0/1/2 in a timed window (emqtt-bench -L semantics are ambiguous, so wall time bounds the run); END-TO-END latency via --payload-hdrs ts + the subscriber's Prometheus e2e_latency HISTOGRAM scraped per scenario. bench/summarize.py renders p50/p99/p999 as bucket upper bounds (1..1000ms resolution — coarse but cannot flatter) + throughput + mem/conn into one markdown table with the dev-grade banner; raw logs remain the record. Verified live (smoke): all 6 scenarios x 3 brokers exit 0; mosquitto verification pass shows ~2000 msg/s aggregate (exactly the configured pub rate), p50<=1ms/p99<=5ms/p999<=25ms, mTLS posture working on all three brokers (EMQX env-mapped verify_peer + fail_if_no_peer_cert; mosquitto require_certificate; mqttd MQTTD_TLS_CLIENT_CA)."
  - id: 0048-T3
    title: The scaling curve — the same workload against 1/3/5 nodes, throughput and p99 vs node count; tests capability claim 1 and the ADR 0015 shared-subscription mechanism end to end; a flat curve is a finding to fix
    status: planned
  - id: 0048-T5
    title: "Durable-path macro-benchmark — end-to-end acked QoS 1/2 throughput and latency against a real quorum with the durable plane on, published in docs/benchmarks/ with method and limits beside every number; the same driver parameterised for multi-host (documented, unrun) and the dangling bench/results citation closed"
    status: done
    date: 2026-08-14
    evidence: "The durable path has end-to-end numbers for the first time. HARNESS: crates/mqttd/tests/durable_bench.rs spawns real production-binary processes (the ADR 0044 P1 proc tier, untouched — an N-node topology is built from its public pieces) with the durable plane ON, and drives closed-loop sliding-window publishers (one packet id per window slot, re-issued on its ack) so the offered rate IS the achieved rate and the measured ack RTT IS the durable-commit latency (ack-after-durable, ADR 0057). Four arms whose DIFFERENCES are the point: qos1 via the session owner, qos1 via a non-owner (ADR 0005 relay), qos1 to CLEAN sessions (the same load with nothing durable to write), qos2 via the owner. Session ids are pinned with the broker's own placement hash so ownership is the test's decision. Warm-up discarded by timestamp; every configuration run 3x with median [min..max] printed; a run JUDGES ITSELF (INVALID on debug profile, publisher errors, durable_append_failures_total moving, publish_dropped{append-backlog-full} moving, min_actual < write_floor, or no completions; caveats for thin samples and driver-bound runs) with per-arm deltas of durable_append_latency_seconds, hub_dispatch_seconds{command='publish'} and append_lane_jobs printed beside the client-side tail. MULTI-HOST: the SAME driver runs against operator-provisioned brokers via MQTTD_BENCH_BROKERS/_HEALTH/_NODE_IDS (nothing is spawned when set); multi_host_preflight exercises parsing + reachability + the readiness gate and prints the exact command when unset. NUMBERS (docs/benchmarks/DURABLE-PATH.md; 8-core macOS dev machine, 3 broker processes + driver on 8 cores and ONE disk, loopback, --release, labelled dev-grade and NOT a multi-host result): every configuration run 3x TWICE (6 reps, median [min..max] across both invocations): serial acked durable qos1 24 msg/s [4..36] at p50 27.6 ms / p99 58.1 ms, vs the same publish to a clean session 30000 msg/s at p50 0.03 ms — the guarantee costs ~900x per message; qos2 9 msg/s at p50 106 ms; at 128 publishes in flight throughput is UNCHANGED (27 msg/s) while p50 becomes 3219 ms, the signature of a hard ceiling. 1 of 6 qos1-owner reps and 2 of 6 qos2 reps came out INVALID and stay in the published ranges. The ceiling is measured too: device_barrier_floor shows File::sync_data() (which is fcntl(F_FULLFSYNC) on macOS, and what redb's Durability::Immediate commits on) runs at 214-234/s serial and 217-238/s with 3 concurrent writers on separate files — PER VOLUME — and store_append_floor shows one durable append costs 5.0 ms with 32-way concurrency no faster than serial (179-203 vs 178-197 appends/s) and 3 stores on one device reaching only 202-227/s. ISOLATION (ADR 0061 / issue #242): 5 nodes, R=3, two nodes' inbound peer bus slowed 1500 ms/chunk, with victim ids in groups whose replica set is {owner, both slowed} and control ids in groups excluding both, BOTH owned by the same node and every role class-pinned (sessions, publishers' own ids, CONNECT probers' own ids) with publishers pre-connected before the clock: the victim class stops completely while the control class keeps publishing FASTER than baseline in all 4 runs (p50 ratio 0.31-0.47x, p95 ratio 0.35-0.50x) and hub_dispatch_seconds{command='publish'} p99 stays <=0.2 ms in all 12 phase measurements — the ADR 0061 mechanism, measured. Stated as prominently: the control class's tail ABOVE p95 could not be attributed on this host (every phase including baseline carried unrelated 10-20s stalls), and a client whose OWN group is degraded pays 24-29s CONNECT p99 — so SIZING's 'connects are unaffected' holds only for clients in healthy groups, and was corrected in place. CITATIONS: bench/results/ is documented in bench/README.md as untracked scratch that nothing may cite; check-readme-facts.py's tracked-citation guard widened from COMPARISON alone to README + docs/benchmarks/*.md (mutation-proven both ways) and now resolves paths against the citing file's own directory. FINDINGS the benchmark surfaced, reported not fixed: inbound QoS 2 hangs silently (no PUBREC, no DISCONNECT) when the publisher's own placement group is owned elsewhere; an idle healthy 3-node cluster produces 1-3 publish completions per 40 s at exactly 5 s or 10 s (the replication RPC bound), twice ending in a withheld ack with no signal to the publisher; the durable append path has no group commit; and /statusz replica_groups never reaches current==tracked on a 5-node cluster (plateaus at 75-85% within 15s and stops), i.e. a permanently non-green operator signal on any cluster that grew. T3 (the scaling curve) and T4 (cross-broker publication) stay planned: no node-count curve and no competitor number is published here."
  - id: 0048-T4
    title: Honesty rules + publication — versions/hardware/config/date stated; losing dimensions reported as prominently as winning ones; results in docs/benchmarks/ linked from the README; self-benchmark runs nightly (ADR 0044 P4), cross-broker re-run per release
    status: planned
---

# Delivery: ADR 0048 — Comparative performance benchmarking

[ADR 0048](../adr/0048-comparative-benchmarking.md) · tasks and status in the frontmatter
above · this file is the plan, progress log, and changelog.

<!-- status-table:0048 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0048-T1 | ✅ done | 2026-07-23 | "bench/: compose profiles run ONE broker at a time (mqttd built from source; Mosquitto 2.0.20; EMQX 5.8.6 — pinned), driven by emqtt-bench 0.6.3 (EMQX's own tool, ADR §3). run.sh executes identical scenarios per broker — connection-rate (timed window; emqtt_bench conn holds connections and never exits, learned in smoke), sustained pub/sub at QoS 0/1/2 (N pubs → N subs, 256 B), RSS snapshot — capturing raw logs + env.txt (versions/params/host, dev-grade label) per run; results/ is gitignored. Posture held constant and disclosed: plaintext/anonymous/in-memory on all three (mqttd explicitly opts out of durable-by-default; TLS posture is T2). Smoke-verified end-to-end on all three brokers: 100/100 connects, publishes complete at every QoS, exit 0." |
| 0048-T2 | ✅ done | 2026-07-24 | "bench/run.sh scenarios per broker in TWO disclosed postures (plaintext 1883, TLS+required-client-certs 8883 from bench/tls/gen-certs.sh — clientAuth EKU, the interop lesson): connection-establishment rate; memory per idle connection (broker RSS snapshotted before/after the conn ramp, settle-delay first); sustained pub/sub at QoS 0/1/2 in a timed window (emqtt-bench -L semantics are ambiguous, so wall time bounds the run); END-TO-END latency via --payload-hdrs ts + the subscriber's Prometheus e2e_latency HISTOGRAM scraped per scenario. bench/summarize.py renders p50/p99/p999 as bucket upper bounds (1..1000ms resolution — coarse but cannot flatter) + throughput + mem/conn into one markdown table with the dev-grade banner; raw logs remain the record. Verified live (smoke): all 6 scenarios x 3 brokers exit 0; mosquitto verification pass shows ~2000 msg/s aggregate (exactly the configured pub rate), p50<=1ms/p99<=5ms/p999<=25ms, mTLS posture working on all three brokers (EMQX env-mapped verify_peer + fail_if_no_peer_cert; mosquitto require_certificate; mqttd MQTTD_TLS_CLIENT_CA)." |
| 0048-T3 | ⬜ planned | — |  |
| 0048-T5 | ✅ done | 2026-08-14 | "The durable path has end-to-end numbers for the first time. HARNESS: crates/mqttd/tests/durable_bench.rs spawns real production-binary processes (the ADR 0044 P1 proc tier, untouched — an N-node topology is built from its public pieces) with the durable plane ON, and drives closed-loop sliding-window publishers (one packet id per window slot, re-issued on its ack) so the offered rate IS the achieved rate and the measured ack RTT IS the durable-commit latency (ack-after-durable, ADR 0057). Four arms whose DIFFERENCES are the point: qos1 via the session owner, qos1 via a non-owner (ADR 0005 relay), qos1 to CLEAN sessions (the same load with nothing durable to write), qos2 via the owner. Session ids are pinned with the broker's own placement hash so ownership is the test's decision. Warm-up discarded by timestamp; every configuration run 3x with median [min..max] printed; a run JUDGES ITSELF (INVALID on debug profile, publisher errors, durable_append_failures_total moving, publish_dropped{append-backlog-full} moving, min_actual < write_floor, or no completions; caveats for thin samples and driver-bound runs) with per-arm deltas of durable_append_latency_seconds, hub_dispatch_seconds{command='publish'} and append_lane_jobs printed beside the client-side tail. MULTI-HOST: the SAME driver runs against operator-provisioned brokers via MQTTD_BENCH_BROKERS/_HEALTH/_NODE_IDS (nothing is spawned when set); multi_host_preflight exercises parsing + reachability + the readiness gate and prints the exact command when unset. NUMBERS (docs/benchmarks/DURABLE-PATH.md; 8-core macOS dev machine, 3 broker processes + driver on 8 cores and ONE disk, loopback, --release, labelled dev-grade and NOT a multi-host result): every configuration run 3x TWICE (6 reps, median [min..max] across both invocations): serial acked durable qos1 24 msg/s [4..36] at p50 27.6 ms / p99 58.1 ms, vs the same publish to a clean session 30000 msg/s at p50 0.03 ms — the guarantee costs ~900x per message; qos2 9 msg/s at p50 106 ms; at 128 publishes in flight throughput is UNCHANGED (27 msg/s) while p50 becomes 3219 ms, the signature of a hard ceiling. 1 of 6 qos1-owner reps and 2 of 6 qos2 reps came out INVALID and stay in the published ranges. The ceiling is measured too: device_barrier_floor shows File::sync_data() (which is fcntl(F_FULLFSYNC) on macOS, and what redb's Durability::Immediate commits on) runs at 214-234/s serial and 217-238/s with 3 concurrent writers on separate files — PER VOLUME — and store_append_floor shows one durable append costs 5.0 ms with 32-way concurrency no faster than serial (179-203 vs 178-197 appends/s) and 3 stores on one device reaching only 202-227/s. ISOLATION (ADR 0061 / issue #242): 5 nodes, R=3, two nodes' inbound peer bus slowed 1500 ms/chunk, with victim ids in groups whose replica set is {owner, both slowed} and control ids in groups excluding both, BOTH owned by the same node and every role class-pinned (sessions, publishers' own ids, CONNECT probers' own ids) with publishers pre-connected before the clock: the victim class stops completely while the control class keeps publishing FASTER than baseline in all 4 runs (p50 ratio 0.31-0.47x, p95 ratio 0.35-0.50x) and hub_dispatch_seconds{command='publish'} p99 stays <=0.2 ms in all 12 phase measurements — the ADR 0061 mechanism, measured. Stated as prominently: the control class's tail ABOVE p95 could not be attributed on this host (every phase including baseline carried unrelated 10-20s stalls), and a client whose OWN group is degraded pays 24-29s CONNECT p99 — so SIZING's 'connects are unaffected' holds only for clients in healthy groups, and was corrected in place. CITATIONS: bench/results/ is documented in bench/README.md as untracked scratch that nothing may cite; check-readme-facts.py's tracked-citation guard widened from COMPARISON alone to README + docs/benchmarks/*.md (mutation-proven both ways) and now resolves paths against the citing file's own directory. FINDINGS the benchmark surfaced, reported not fixed: inbound QoS 2 hangs silently (no PUBREC, no DISCONNECT) when the publisher's own placement group is owned elsewhere; an idle healthy 3-node cluster produces 1-3 publish completions per 40 s at exactly 5 s or 10 s (the replication RPC bound), twice ending in a withheld ack with no signal to the publisher; the durable append path has no group commit; and /statusz replica_groups never reaches current==tracked on a 5-node cluster (plateaus at 75-85% within 15s and stops), i.e. a permanently non-green operator signal on any cluster that grew. T3 (the scaling curve) and T4 (cross-broker publication) stay planned: no node-count curve and no competitor number is published here." |
| 0048-T4 | ⬜ planned | — |  |
<!-- /status-table:0048 -->

## Plan

| Task | Done means |
|---|---|
| **0048-T1** Harness | `docker compose up` reproduces the comparison: each broker from its image, one load tool, one hardware profile, disclosed configs, constant security posture. |
| **0048-T2** Metrics | Throughput, latency (p50/p99/p999), memory/connection at scale, and mTLS connection rate — each with its distribution and the load that produced it. |
| **0048-T3** Scaling curve | Throughput + p99 vs 1/3/5 nodes, published; the linear-scaling claim earned or the gap surfaced. |
| **0048-T4** Honesty + publish | Results (with versions/hardware/date, wins and losses) in `docs/benchmarks/`; self-benchmark nightly, cross-broker per release. |

Order: T1 → T2 → T3 → T4.

## Phased execution plan

Phased so **each step delivers value even if we stop there**, and so cost is deferred to the
last responsible moment — the harness and the numbers that *guide* us cost nothing; only the
numbers we *publish* cost money.

| Phase | Task | Cost | Output |
|---|---|---|---|
| **1. Harness** | T1 | none — start now | Containerized rig: fss / Mosquitto / EMQX from **pinned published images**, documented *reasonable* configs (theirs not crippled, ours not tuned), driven by **`emqtt-bench`** (EMQX's own load tool — a built-in honesty signal). **Two postures per broker: plaintext and TLS/mTLS**, disclosed. |
| **2. Dev-grade numbers** | T2 | none — local | Throughput QoS 0/1/2, latency p50/p99/p999, memory per 10k idle connections, mTLS connect rate — **full distributions**. Run on a workstation, labeled **development-grade**: they *guide* decisions, they are **not published and never quoted**. |
| **3. Publishable run** | T2 | small — one rented box for an afternoon (optionally two: driver + broker) | The same metrics, pinned everything, **raw output committed**. The **only** step with a cash cost, and the **only** numbers that go into `docs/benchmarks/`. |
| **4. Scaling curve** | T3 | small — 3–5 small cloud VMs for hours | 1/3/5-node throughput + p99 vs node count, **on separate hosts with independent disks**. A durable cluster is fsync-bound (ADR 0026/0027); a single-host curve would scale *negatively* and manufacture false evidence against us — so this runs on real separate hosts or it is not published. |
| **5. Publish** | T4 | none | `docs/benchmarks/` with versions/hardware/date, **losses printed as prominently as wins**, README Performance section links it. Nightly self-benchmark (ADR 0044 P4) guards regression; the cross-broker comparison is re-run per release. |

**The dev-grade / publishable line is the crux of the honesty story:** local numbers are
cheap and plentiful but run on shared, noisy, un-pinned hardware, so they steer the work
without ever becoming a quotable claim. A number only earns publication once it comes from
the pinned, dedicated, disclosed environment of phases 3–4.

## Changelog

- **2026-08-14 (review round, before commit)** — adversarial verification of 0048-T5 found
  five honesty defects in the published artifact and they are corrected in place. (a) The
  headline arithmetic did not close: "~215-240 barriers/s / ~2 per message ~= ~30-120 msg/s"
  is wrong division (that is ~110-120), and the measured 24-28 msg/s sits OUTSIDE the range
  the text claimed it "sits inside". It now states the ceiling honestly (~110-120), says the
  measurement is a factor of four below it, and derives what the two numbers jointly imply
  (~8-10 barriers per acked message, an inference from two measurements and labelled as
  one) instead of pretending to a closed derivation. (b) SIZING reported only the isolation
  metrics that IMPROVED (control p50/p95 faster) and omitted that the control class's own
  throughput fell to 0.41-0.77x in the same runs — the faster latency is a CONSEQUENCE of
  the lower throughput, so the property demonstrated is isolation of failure, not of
  capacity; both are now stated together, with "five processes on one host" beside them.
  (c) The `append_lane_jobs` figures were used to support "nothing was blocking the loop",
  but the harness scrapes that gauge once, after every publisher has stopped: it shows lanes
  had drained, not how deep they got. The claim now rests on the dispatch histogram alone,
  which is a histogram over every dispatch and can carry the argument. (d) The isolation
  table's "recovery when healed" row — the only row with no numbers — did NOT reproduce
  (re-run gave control 0.63x and victim 0.61x of baseline, healed-control p99 5113 ms vs
  299 ms); the row now says so and the document declines to claim recovery either way.
  (e) `driver_bound()` returned `false` when no broker CPU was available, which is exactly
  the external multi-host lane, so an UNRUN check read as a passed one; it is now
  three-valued and prints "check DID NOT RUN" with the driver's CPU-seconds. Also: issue
  #244's first acceptance criterion is only partly met by the CI guard — two citations of the
  gitignored `bench/results/results.md` survive in 0051-T9's dated evidence, and
  `docs/delivery/` is deliberately outside the guard because rewriting dated evidence would
  be the dishonest fix, so that class is corrected by dated forward pointers (one now sits on
  0051-T9) rather than by the check. And the connection dimension #244 also asks about is now
  named as unbounded here: every number was taken at <=32 concurrent connections, and
  connection capacity is `bench/run.sh`'s `conn` scenario, a different measurement.

- **2026-07-17** — ADR 0048 drafted. Differentiation/credibility: "Fast" and "linearly
  scalable" are in the product's own name but unproven; extends ADR 0044 P6's internal
  baselines to published, reproducible, self-critical cross-broker numbers. Priority **P2**.
- **2026-07-19** — Phased execution plan added (above), and two decision-level refinements
  folded into the ADR:
  - **`emqtt-bench` named as the load driver** — measuring ourselves with EMQX's own tool is
    an honesty signal; each broker measured in two disclosed postures (plaintext + TLS/mTLS).
  - **The scaling curve must run on separate hosts/disks.** A durable cluster is fsync-bound
    (ADR 0026/0027 — group-commit exists because per-message follower fsyncs were the
    bottleneck); a single-host N-node curve contends on one disk queue, scales negatively, and
    would publish false evidence *against* the broker. Curve runs on real separate hosts or not
    at all.
  Cost stays bounded: phases 1–2 (harness + dev-grade local numbers) are free and only guide;
  the sole cash outlay is the one publishable run (a rented box) plus a few VM-hours for the
  curve. Tasks remain **planned** — this is planning, not execution.
- **2026-07-19** — The single-host lesson is now backed by its primary source: the
  [7-node HA-bridge post-mortem](../postmortems/2026-07-14-ha-bridge-durable-refused.md) is
  filed under `docs/postmortems/` and cited from the ADR's scaling-curve decision. (The
  post-mortem also surfaces two real defects — learner-owner durable recovery, and a
  readiness blind spot — tracked separately, not by this ADR.)
