---
adr: "0043"
title: Elastic cluster resize (grow, shrink, replace)
adr_status: Proposed
tasks:
  - id: 0043-P1
    title: Catch-up replicas — a node entering a group's replica set back-fills the group's log (ReplicaKeys discovery + recover_key quorum reads) behind a durable caught-up watermark, and counts toward NO quorum (append or recovery) until caught up; growing 1→N back-fills laptop-mode single-replica history as the same rule
    status: done
    date: 2026-07-13
    evidence: "Recovery reads now carry a COMPLETENESS verdict with two halves: per-key gap-freedom (ReplicaState::complete — stored offsets contiguous above the truncation low-water, so a joiner holding new appends above a hole it never received reads incomplete) AND the durable caught-up watermark (new replica_caught_up redb table: per group, the replica set this node last completed catch-up against; a stamp from a different cohort — or no stamp — reads incomplete, so an EMPTY joiner cannot claim a key has no history; additive table, no schema bump, survives restart so a crash mid-catch-up resumes rather than fakes completion). recover_key requires a quorum PLUS at least one complete anchor, and the OWN copy obeys the same rule — an unstamped owner cannot fabricate an empty log (fails closed with NoQuorum until its boot sweep stamps; ADR 0017 posture). The back-fill: a catch-up sweep in the lease driver (armed at boot and on every placement membership change, CATCH_UP_SWEEP_EVERY=5 ticks, budget 24) discovers keys per-peer (ReplicaKeys, attributed — a group stamps only once every other set member was heard), heals hollow keys by asking the group owner to recommit_key them (new ReplicaCatchUp frame; owner-side CatchUpSource seam on the plane, implemented by GroupRoutedLog: route → recover → re-commit at the owner epoch, idempotent and fenced, plus a truncation-floor fan so drained prefixes read as acked-away rather than as gaps), and re-stamps pure shrinks of a known cohort immediately (custody unchanged — no data moved on 5→3). Cached group logs rebuild on REPLICA-SET changes, not only epoch changes, so appends and re-commits fan out to joiners. Wire: proto 3→4; ReplicaRead2/ReplicaReadReply2 (request version selects reply shape; pre-proto-4 replies are conservatively incomplete) and ReplicaCatchUp appended at the enum end (ADR 0038 compat). Tests — unit: completeness shapes (gap/hole/truncated-floor), stamp currency + reopen persistence, unstamped-owner-cannot-fabricate, recovery-refuses-a-quorum-of-hollow-replicas (then serves once a complete anchor joins), growing_a_one_node_group_back_fills_the_joiners (store seam), proto4 wire round-trip + catch-up routing (plane seam). Integration (cluster_stress): growing_one_node_to_three_back_fills_and_survives_the_founder — one durable node accumulates acked facts (offline durable session owed 3 acked QoS1 payloads + acked retained), grows 1→3, both joiners reach the caught-up watermark on q/, m/ and r/ keys, the FOUNDER IS KILLED (the only pre-grow copy), and the session resumes present=true on the survivors with every acked payload replayed and the retained value served — zero loss, zero waivers (~17s). Workspace green (all suites), clippy zero warnings."
  - id: 0043-P2
    title: Eager migration on ring change — membership growth triggers the takeover-scan materialization (sessions, retained, interest) for groups whose owner moved, instead of first-touch; the settle/re-route + mesh-whole ack machinery holds acks honest during the window
    status: planned
  - id: 0043-P3
    title: Decommission — an explicit drain (stop new sessions → successors caught up + replica counts restored among remaining members → voter demotion → ADR 0019 leave), observable via the health endpoint, interruptible (crash mid-drain = crash); operator trigger chosen here (signal vs admin endpoint)
    status: planned
  - id: 0043-P4
    title: Resize test vocabulary — join/decommission steps in the ADR 0042 stress schedules, plus dedicated upgrade-path tests (1→3 laptop-to-server, 3→5 zone-spread, 5→3 cost reduction, rolling host replacement, rolling binary upgrade across the proto window) under the unchanged acked-obligations oracle
    status: planned
  - id: 0043-P5
    title: Operator docs — the "grow your broker" guide (one paragraph per direction), the two-node-waypoint honesty note (quorum 2-of-2; recommend 1→3), README interim warning that durable resize is unsupported until P1 lands
    status: planned
---

# Delivery: ADR 0043 — Elastic cluster resize

[ADR 0043](../adr/0043-elastic-cluster-resize.md) · tasks and status in the frontmatter
above · this file is the plan, progress log, and changelog.

<!-- status-table:0043 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0043-P1 | ✅ done | 2026-07-13 | "Recovery reads now carry a COMPLETENESS verdict with two halves: per-key gap-freedom (ReplicaState::complete — stored offsets contiguous above the truncation low-water, so a joiner holding new appends above a hole it never received reads incomplete) AND the durable caught-up watermark (new replica_caught_up redb table: per group, the replica set this node last completed catch-up against; a stamp from a different cohort — or no stamp — reads incomplete, so an EMPTY joiner cannot claim a key has no history; additive table, no schema bump, survives restart so a crash mid-catch-up resumes rather than fakes completion). recover_key requires a quorum PLUS at least one complete anchor, and the OWN copy obeys the same rule — an unstamped owner cannot fabricate an empty log (fails closed with NoQuorum until its boot sweep stamps; ADR 0017 posture). The back-fill: a catch-up sweep in the lease driver (armed at boot and on every placement membership change, CATCH_UP_SWEEP_EVERY=5 ticks, budget 24) discovers keys per-peer (ReplicaKeys, attributed — a group stamps only once every other set member was heard), heals hollow keys by asking the group owner to recommit_key them (new ReplicaCatchUp frame; owner-side CatchUpSource seam on the plane, implemented by GroupRoutedLog: route → recover → re-commit at the owner epoch, idempotent and fenced, plus a truncation-floor fan so drained prefixes read as acked-away rather than as gaps), and re-stamps pure shrinks of a known cohort immediately (custody unchanged — no data moved on 5→3). Cached group logs rebuild on REPLICA-SET changes, not only epoch changes, so appends and re-commits fan out to joiners. Wire: proto 3→4; ReplicaRead2/ReplicaReadReply2 (request version selects reply shape; pre-proto-4 replies are conservatively incomplete) and ReplicaCatchUp appended at the enum end (ADR 0038 compat). Tests — unit: completeness shapes (gap/hole/truncated-floor), stamp currency + reopen persistence, unstamped-owner-cannot-fabricate, recovery-refuses-a-quorum-of-hollow-replicas (then serves once a complete anchor joins), growing_a_one_node_group_back_fills_the_joiners (store seam), proto4 wire round-trip + catch-up routing (plane seam). Integration (cluster_stress): growing_one_node_to_three_back_fills_and_survives_the_founder — one durable node accumulates acked facts (offline durable session owed 3 acked QoS1 payloads + acked retained), grows 1→3, both joiners reach the caught-up watermark on q/, m/ and r/ keys, the FOUNDER IS KILLED (the only pre-grow copy), and the session resumes present=true on the survivors with every acked payload replayed and the retained value served — zero loss, zero waivers (~17s). Workspace green (all suites), clippy zero warnings." |
| 0043-P2 | ⬜ planned | — |  |
| 0043-P3 | ⬜ planned | — |  |
| 0043-P4 | ⬜ planned | — |  |
| 0043-P5 | ⬜ planned | — |  |
<!-- /status-table:0043 -->

## Plan

| Task | Done means |
|---|---|
| **0043-P1** Catch-up replicas | A replica-set joiner back-fills before counting toward any quorum, behind a durable watermark that survives restart; the hollow-replica recovery hazard (a "quorum" of {empty joiner, lagging survivor} silently truncating) is closed by construction; growing a 1-node broker re-replicates its history. Unit tests at the cluster_log/cluster_store seams + a stress-harness join under load. |
| **0043-P2** Eager migration | A ring change materializes moved groups (sessions, retained, interest) without waiting for first touch — the ADR 0042 T9 takeover scan generalized to membership growth; acks stay honest through the window via the existing settle/re-route + mesh-whole rules. |
| **0043-P3** Decommission | An operator-triggered drain completes with every owned group handed to a caught-up successor and every replicated group at full replica count among the survivors, then demotes and leaves; progress observable, crash-safe at every point. |
| **0043-P4** Resize vocabulary | The stress harness composes join/decommission into seeded schedules; dedicated tests drive 1→3, 3→5 (zone-spread), 5→3, rolling replacement, and a rolling binary upgrade; all hold the acked-obligations oracle with zero waivers. |
| **0043-P5** Operator docs | The grow/shrink/replace guide ships with the two-node honesty note; the README carries an interim "durable resize unsupported" warning until P1 lands, removed by this task. |

Order: P1 → P2 → P3 (each unblocks the next), P4 grows alongside each, P5 last.

## Exhibits / findings ledger

| # | Finding | Where | Status |
|---|---|---|---|
| — | 2026-07-13 inventory: consensus-plane resize built and unit-tested (ADR 0016/0021/0026/0028); data-plane resize missing — hollow replicas count toward quorum, ownership moves are first-touch, graceful leave hands off nothing, laptop-mode history never re-replicates; no integration test resizes a running durable cluster | code/test survey (see ADR context) | open — this ADR is the plan |

## Changelog

- **2026-07-13** — **0043-P1 done: catch-up replicas.** A recovery read now carries a
  **completeness verdict** — gap-free stored offsets above the truncation low-water
  *and* a durable per-group **caught-up stamp** (`replica_caught_up` table: the replica
  set this node last completed catch-up against; survives restart, so a crash
  mid-catch-up resumes rather than fakes). `recover_key` requires a quorum **plus at
  least one complete anchor**, which closes both faces of the hollow-replica hazard: a
  joiner holding new appends above a hole it never received (gap ⇒ incomplete), and an
  **empty** joiner claiming a key has no history (no stamp ⇒ incomplete — an unstamped
  owner cannot fabricate an empty log either). The back-fill itself: a catch-up sweep in
  the lease driver (armed at boot and on every membership change) discovers keys
  per-peer (`ReplicaKeys`), asks each hollow key's owner to `recommit_key` it
  (`ReplicaCatchUp`, proto 4) — the existing fenced idempotent re-commit — plus a
  truncation-floor fan so drained prefixes read as acked-away, not as gaps; groups
  re-stamp when every discovered key is gap-free and every other set member was heard
  (pure shrinks of a known cohort re-stamp immediately — custody unchanged). Cached
  group logs now rebuild on **replica-set changes** too, so appends and re-commits fan
  out to joiners. Wire: proto 3→4, `ReplicaRead2`/`ReplicaReadReply2`/`ReplicaCatchUp`
  appended (pre-proto-4 peers keep the legacy read; their replies are conservatively
  incomplete). Proven end to end by the harness: grow 1→3 under acked facts, wait for
  the joiners' watermarks, kill the founder — session resumes present, every acked
  payload replays, the acked retained value serves (zero waivers).
- **2026-07-13** — ADR 0043 drafted with delivery plan P1–P5, from the cluster-resize
  inventory (consensus half ready, data half missing). Motivated by the capability
  plan's "adding a node adds throughput" claim and the laptop→server upgrade sell.
