# Post-mortem: HA-bridge demo — persistent sessions refused (0x88) on the 7-node demo cluster

**Date:** 2026-07-14 · **Duration:** entire stack lifetime, 06:25–17:31 UTC (~11 h) · **Status:** stack torn down; evidence was preserved in `/tmp/bridge-ha-postmortem/` (ephemeral — not committed; this record is the durable summary)

> **Filed:** 2026-07-19. Related decisions: [ADR 0017](../adr/0017-durable-attach-readiness.md)
> (durable attach fail-closed), [ADR 0021](../adr/0021-bounded-lease-voters.md) (bounded lease
> voters + learner ownership — see follow-up 1, which contradicts its §2), [ADR 0026](../adr/0026-lease-timing-durable-storage.md)
> / [ADR 0027](../adr/0027-replica-group-commit.md) (fsync-bound commit path), [ADR 0020](../adr/0020-metrics-and-observability.md)
> (readiness model — see follow-up 2), [ADR 0048](../adr/0048-comparative-benchmarking.md)
> (the scaling curve must run on separate hosts *because* of this incident).

## Summary

The HA boundary-bridge pair (`bridge-1`/`bridge-2`, shared group `boundary-ha`) could never
attach to the cluster: every persistent-session CONNECT was refused with CONNACK 0x88
(Server unavailable). This was **not a bridge bug** — *all* durable (persistent) sessions
cluster-wide were refused for the stack's entire life, while clean-session traffic worked
perfectly. The demo's readiness endpoints reported `ok` throughout.

## Impact

- Bridge HA demo unusable (both instances stuck in a connect-retry loop; partner side connected fine).
- Every other persistent session too (loadgen's `demo-sub` included) — verified with fresh probes on two nodes.
- Clean sessions, QoS routing, QUIC, WS, metrics: all unaffected.

## Timeline (UTC)

| Time | Event |
|---|---|
| 06:25:32 | Stack starts (7 nodes, durable-by-default, `voter_cap=5`) |
| 06:25–06:27 | SWIM mesh + all peer links established; elections T1→T10; mqttd-3 becomes leader |
| **06:27:30** | **Leader's `AppendEntries` RPCs begin timing out (500 ms deadline) — to all 6 peers, intermittently. Never stops.** |
| 06:25 onward | Both bridges refused 0x88 on every attempt (formation at first, then the degraded state below) |
| 07:38 | Re-election T10→T12 (vote RPCs *did* get through); T12 then holds for 10 h |
| 15:23–15:56 | Investigation: cluster "ready", bridges still refused; probes prove durable attach broken cluster-wide |
| 17:31 | Evidence captured; snapshot shows **mqttd-3 (leader) `/readyz` hung** while its `/metrics` answers; teardown |

## What the evidence shows

1. **Leader replication degraded from t+2 min, forever.** mqttd-3 logged ~24,200
   `RPCError err=timeout after 500ms when AppendEntries` — ~4,000 per target, *evenly across
   all six* peers (voters mqttd-1/4/6/7 and learners mqttd-2/5), spread over every hour
   (288–4,986/h). No sleep gap exists in the logs (max inter-line gap < 60 s) — the earlier
   "laptop slept" theory is **disproved**; the "gap" was only the bridges' connect backoff.
2. **The network was fine.** Zero peer-link failures logged anywhere; links stayed up 11 h;
   vote RPCs and MQTT forwarding over the same links worked. Same-host Docker networking
   (sub-ms RTT). The 500 ms deadline was being missed in *processing*, not transit.
3. **Quorum survived on the tail that got through.** Elections settled (T12 for 10 h), epoch
   stable, so `lease_group_ready=true` on all five voters → `/readyz ok`. The health model
   samples *leadership + voter membership*, not RPC health — a **green-but-degraded** state.
4. **Durable session recovery is deadline-bound and compound.** `recover_until_ready` retries
   `claim_session` for `ATTACH_RECOVERY_TIMEOUT = 5 s`, treating `Unavailable/NotOwner/NoQuorum`
   as transient, then rejects (ADR 0017, correct fail-closed). Recovery needs several timely
   peer-bus round-trips (lease claim at the owner + replicated-log reads). Against a peer bus
   with a heavy slow-tail, the compound op reliably exceeded 5 s → 0x88, every time.
5. **Placement can pick permanent learners as owners.** `probe-durable-3`'s owner was
   **mqttd-5 — a permanent learner** (voter_cap=5 < 7 nodes). A learner can never hold the
   lease, so any session whose id hashes to it can *never* recover (deadline is irrelevant).
   `fss-bridge-1`'s owner was mqttd-4 (a voter) and still failed — so learner-placement is an
   *additional* failure mode on top of (4), not the sole cause.
6. **Leader's health endpoint wedged.** At capture time mqttd-3's `/readyz` returned nothing
   (curl timeout) while `/metrics` on the same server answered — unexplained; worth its own look.

## Root cause (assessed)

**Primary:** 7 durable nodes on one laptop (Docker Desktop VM) multiply the fsync-bound
consensus/replication load (~2.3× the 3-node demo: raft log + session log + retained store
per node, one shared VM disk). Follower-side `AppendEntries` handling — which persists before
answering — intermittently exceeds the 500 ms RPC deadline. The lease group tolerates a lossy
heartbeat tail (ADR 0026 timing), so leadership holds; but **durable session recovery, which
needs a chain of timely round-trips within 5 s, sits past the tipping point and fails 100%**
of the time. Correlation: error-rate peaks track host activity (builds/tests ran on the same
machine during the day).

**Secondary (independent):** with `voter_cap` (5) < cluster size (7), **session placement
still hashes across all 7 SWIM members**, so ~2/7 of session ids get a permanent-learner owner
that can never serve durable recovery. Those ids fail even on a healthy cluster.

**Tertiary (observability):** `/readyz` conflates "lease group has a leader and I'm a voter"
with "durable plane is serviceable." A cluster that cannot attach a single persistent session
reported green for 11 hours. `mqttd_durable_append_failures_total` stayed at zero — recovery
failures are not appends, so no metric moved.

## What went well

- **Fail-closed held everywhere** (ADR 0017): no session was silently downgraded to clean;
  clients got a retryable 0x88. No data-integrity lie was told.
- The bridge behaved exactly as designed: partner side up, cluster side retried with backoff,
  spool intact.
- Clean-session MQTT service was unaffected throughout.

## Follow-ups (proposed)

1. **Placement × voter-cap (bug, main):** durable-session ownership must be restricted to
   lease-eligible (voter-capable) nodes, or learners must proxy recovery to the lease group —
   otherwise every N>voter_cap cluster has a deterministic slice of unusable session ids.
   *(Same root as the earlier `/readyz`-503-forever-on-learners finding — one ADR should own both.)*
   **Note:** [ADR 0021 §2](../adr/0021-bounded-lease-voters.md) asserts a learner *can* own a
   group and run its `ClusterLog` "exactly as a voter would" — this incident shows that path
   does not actually serve durable recovery, so the design record and reality disagree.
2. **Readiness/metrics blind spot (main):** surface consensus RPC health — e.g. a
   `mqttd_lease_rpc_timeouts_total` counter and/or a `durable_recovery_failures_total` — and
   consider gating `lease_group_ready` on recent replication success, not just leadership.
3. **Demo sizing note (demo):** document that ≥5 durable nodes on a single host is
   fsync-bound and may degrade exactly this way; consider `MQTTD_LEASE_VOTERS=<N>` or fewer
   nodes as the default demo, or a compose healthcheck that probes a real persistent attach.
4. **Leader `/readyz` hang (investigate):** metrics served while readyz hung on the leader.
5. **Stale log text (trivial):** `hub.rs` `note_session_ownership` says "(ephemeral mode)" on
   a durable cluster — misleading during exactly this kind of incident.

## Evidence index (`/tmp/bridge-ha-postmortem/`, ephemeral — not committed)

- `mqttd-{1..7}.log`, `bridge-{1,2}.log`, `loadgen.log`, `partner-broker.log` — full container logs
- `state-snapshot.txt` — readyz + lease/epoch/members per node at capture time
- Key numbers: 24,210 leader RPC timeouts; ~4,000/target × 6 targets; T12 stable 10 h;
  raft-id map: mqttd-1=17539…, -2=17106…, -3=22178… (leader), -4=16509…, -5=16631…, -6=15287…, -7=10078…
