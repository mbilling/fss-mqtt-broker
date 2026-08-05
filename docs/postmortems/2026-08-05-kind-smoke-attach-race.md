# Post-mortem: kind runtime smoke — retained read lost to a `kubectl attach` race (harness defect, not a broker defect)

**Date:** 2026-08-05 · **Environment:** ADR 0047 `kube-smoke` run locally on macOS/arm64 (Docker Desktop VM, kind v1.34 node, 3-node StatefulSet, `lease_voters=3`) · **Status:** root cause confirmed and FIXED (attach-free reader); the broker needed no change

> Related: [ADR 0047](../adr/0047-kubernetes-deployment.md) (the smoke),
> [ADR 0037](../adr/0037-durable-retained-messages.md) (the pipeline wrongly suspected),
> [ADR 0054](../adr/0054-operator-facing-state-surface.md) (`/statusz` + gauges, which
> did the diagnosis). Sibling: [2026-07-20](2026-07-20-kube-retained-ownership-split.md) —
> same *symptom* (retained read-back empty), genuinely a broker bug that time. That
> precedent is exactly why this one was initially misdiagnosed the same way.

## Summary

The first local run of `scripts/k8s/kind-smoke.sh` on an arm64 Mac failed at
`FAIL: retained message not delivered`, reproducibly (2/2). The same commit's nightly
`kube-smoke` on amd64 CI was green. It was filed as a broker bug
([#86](https://github.com/mbilling/fss-mqtt-broker/issues/86)) on the hypothesis that a
durably-committed retained value was reaching a node's store but never its subscribers.

**That hypothesis was wrong.** The retained pipeline was working correctly on every node.
The smoke's reader used `kubectl run --rm -i`, which *attaches* to the client container;
when the retained replay is fast the client prints and exits **before the attach
completes**, and kubectl loses the output. On amd64 runners the attach usually wins that
race; on this arm64 VM it often does not.

## What the evidence actually showed

| Probe | Result |
|---|---|
| `/statusz` on all three pods | one `cluster_id`, 3 members, voters = all three, leader = pod-0, `replica_groups 256/256 current`, no brownout |
| `mqttd_retained_messages` after 7 retained publishes | **7 on every pod** — all caches fully converged |
| 5 subscribes run as plain pods, output read from `kubectl logs` | **5/5 success**, each with a clean trace: `CONNACK (0)` → `SUBACK … Subscribed (mid: 1): 0` → `PUBLISH (… r1 …)` → payload |
| The same read via `kubectl run --rm -i` | **3/6 hits**; misses returned in ~2 s, *not* at the `-W 15` timeout — the subscriber never waited |
| The tell, present in the very first logs | `warning: couldn't attach to pod/…: container … not found in pod …` |

The `~2 s` return on a miss is the decisive datum: a genuinely absent retained message
would have held the subscriber until its timeout. Exiting early with no output means the
client had already finished and its stdout was discarded.

## Root cause

`kubectl run -i` establishes an attach *after* the pod starts. A container that completes
before the attach is established loses its stdout — kubectl falls back to streaming logs,
but the write has already happened. A fast retained replay (single message, `-C 1`) is
exactly the workload that loses this race.

## Fix

`mqtt()` in `scripts/k8s/kind-smoke.sh` no longer attaches. It runs the client pod to
completion, waits for a terminal phase, and reads `kubectl logs` — deterministic,
attach-free, same cost. The helper carries a comment recording *why*, so the `-i` form
is not reintroduced. Smoke now passes locally on arm64 (2 consecutive runs) and is
unchanged for CI.

## Why this took a full investigation

The symptom was identical to the 2026-07-20 incident, which *was* a real
ownership/routing bug. Pattern-matching to the prior post-mortem made a broker defect the
leading hypothesis, and three of the plausible-looking mechanisms (unacked fan-out,
token-before-store ordering, link-up-only anti-entropy) turned out to exist in the code —
they simply were not what was happening here. They are filed separately as
[#87](https://github.com/mbilling/fss-mqtt-broker/issues/87).

## Lessons

1. **A test-harness defect can perfectly imitate a distributed-systems bug.** Before
   accepting "the cluster lost data", confirm the *observation path* is sound. The check
   that settled it was cheap: read the same state a second, independent way.
2. **"Failed fast" is diagnostic.** A read that returns well before its own timeout did
   not wait, so it did not observe an absence — it lost its output. Timing anomalies in a
   negative result deserve as much attention as the negative result.
3. **Arch/environment differences surface harness races, not just performance
   differences.** A local run on a different architecture is a genuinely different test
   of the *test*.
4. **Never use `kubectl run -i` to capture output from a short-lived pod.** Run to
   completion and read logs.
5. The ADR 0054 signals paid for themselves immediately: `/statusz` and the retained
   gauges are what proved the cluster healthy in minutes rather than hours.
