# ADR 0042 — Durable-plane stress and simulation harness

- **Status:** Proposed
- **Date:** 2026-07-10
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0042-durable-plane-stress-harness.md](../delivery/0042-durable-plane-stress-harness.md) — plan, progress, and changelog
- **Related:** [ADR 0024](0024-deterministic-testing.md) (deterministic testing — the SWIM
  simulation harness this extends, and the recorded deferral it pays off),
  [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (the lease consensus + quorum replication under test), [ADR 0017](0017-durable-attach-readiness.md)
  (recovery honesty — attach never fabricates a clean session), [ADR 0018](0018-on-disk-persistence.md)
  (the redb stores whose crash/restart behavior is exercised), [ADR 0037](0037-durable-retained-messages.md)
  (retained convergence tokens — the newest invariants under test), [ADR 0021](0021-bounded-lease-voters.md)
  (voter/learner topology the schedules must cover), [ADR 0041](0041-resource-governance.md)
  (brownout — a fault mode the harness injects; its T1 evidence records the first exhibit)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0042-durable-plane-stress-harness.md).

## Context

The durable plane — the ownership-lease consensus (openraft, ADR 0006), the quorum-replicated
session log (ADR 0007/0018), epoch-fenced takeover recovery (ADR 0017), and the durable
retained authority with its convergence tokens (ADR 0037) — is the broker's hardest
correctness surface: it is where a bug does not drop a packet but **loses acknowledged data,
resurrects deleted state, or lets two nodes both believe they own a session**. It is also the
layer where the pre-release program has the least systematic evidence:

- **Its tests are scenario tests.** `cluster.rs`, `cluster_chaos.rs`, `durable_sessions.rs`,
  `persistence.rs`, and the in-crate `mqtt-cluster` tests each script *one* fault at *one*
  point (one partition, one takeover, one restart) and assert one outcome. They prove the
  happy paths and the specific faults someone thought of; they say nothing about fault
  *schedules* — a partition during a takeover during a back-fill — where distributed bugs
  actually live.
- **They synchronize on real time and real load.** The suite already produced a
  load-dependent flake in exactly this layer:
  `a_takeover_recovers_the_retained_value_and_its_token` failed once under full-workspace
  parallel load and passed every isolated rerun (recorded in ADR 0041's T1 evidence as an
  exhibit for this ADR). Today a flake like that costs a debugging session under
  CI-only conditions, and "passed on rerun" is the only verdict available.
- **The deterministic-testing program stopped at the door.** ADR 0024 built the
  seed-reproducible simulation harness for SWIM (`swim_sim.rs`: virtual clock, simulated
  network, every choice from one seed, a failure reruns identically) and explicitly deferred
  the lease/replication layer as "async-I/O-entangled — the natural extension once a seam
  exists". That deferral is this ADR.
- **The invariants are folklore.** "An acked write survives takeover", "a stale epoch is
  fenced everywhere", "a cleared retained topic never resurrects" are each asserted
  *implicitly* by some scenario test, but there is no single executable statement of what
  the durable plane guarantees — so no way to check *all* of it after *any* fault schedule.

This is pre-release area ④ (the plan recorded in ADR 0038's changelog). It goes last
because it tests what ①–③ froze and built; it must land before release because the first
release turns the durable plane's behavior under faults into an operational promise.

## Decision

**State the durable plane's invariants once, as executable checkers; verify them
deterministically where the layer is pure and under seed-reproducible fault schedules where
it is not; gate a bounded profile in CI and keep a soak profile for depth. Every failure
prints the seed that reproduces its schedule.**

### 1. An invariant catalog, as code

The durable plane's guarantees become one executable catalog — checker functions over
observable state, not prose — asserted by every harness scenario and available to the
existing scenario tests:

- **Acked durability:** a message acknowledged to a QoS ≥ 1 publisher (or an accepted
  retained mutation) survives any single-fault schedule the plane is specified to tolerate
  — owner crash, takeover, follower loss within quorum, full-cluster restart.
- **Epoch fencing:** at most one holder per placement group is accepted as writer;
  a deposed holder's writes at a stale epoch are refused by every replica; epochs are
  strictly increasing.
- **Session singularity:** one client id has at most one live session cluster-wide;
  takeover closes the old before the new serves.
- **Recovery honesty (ADR 0017):** an attach never fabricates a clean session over a
  recoverable one — the durable answer is `present`, `absent`, or a loud `Unavailable`,
  never a silent wrong one.
- **Retained convergence (ADR 0037):** after heal, every node's retained cache equals the
  authority's; per-topic tokens are monotonic; a cleared topic never resurrects from a
  staler value.
- **Bounded structures stay bounded** under churn (queues, tables, and maps hold their
  ADR 0041 bounds through fault schedules, not just in steady state).

The catalog is the oracle; scenarios only choose *what to do*, never *what must hold*.

### 2. Deterministic simulation of the pure core

The plane's own state machines are already pure and clock-free — `LeaseMap::apply`
(lease assignment), the `cluster_log` replica/fencing logic, the retained token
application and replay rules, HRW placement. These get the `swim_sim` treatment
**unchanged in style**: a seeded schedule generator (the codebase's hand-rolled xorshift;
no new dependencies) drives them through reorderings, duplications, drops, and
interleavings; the invariant catalog is asserted after every step; each scenario runs
across many seeds and a violation panics with the seed, which reruns the identical
schedule (`REPRO_SEED` to focus it). openraft's *internals* are explicitly not simulated —
it is the ratified engine (ADR 0006); what we simulate is **our** state and **our** glue
around it: fencing, application order, replay, back-fill — where our bugs live.

### 3. Seed-reproducible stress over real nodes

The whole plane — openraft, tokio, redb, the peer mesh — cannot be made a pure function
without rewriting the runtime, so the whole-plane layer is **stress, honestly labelled**:
an in-process multi-node cluster (the existing test-harness node builders) driven by a
seeded **fault schedule** composed from bounded primitives — node kill/restart,
link partition/heal, peer-frame delay/drop, client churn, takeover storms, disk faults
(§4) — interleaved with a seeded **workload** (publishers, subscribers, resumes, retained
mutations, QoS mix). After the schedule: quiesce, heal everything, and run the full
invariant catalog plus convergence checks. One seed determines every schedule and workload
choice; tokio's scheduler and real I/O mean replay is best-effort rather than identical —
the seed reproduces the *scenario*, and the run logs enough (schedule trace, per-node
state digests) that a failure is a bug report, not a shrug. This is the layer that hunts
the class the recorded takeover flake belongs to.

### 4. Crash, restart, and disk faults are first-class schedule entries

The fault vocabulary includes what the wall-clock scenario tests cover thinnest: process
kill (not graceful shutdown) with the redb data dir surviving into a restarted node;
full-cluster stop/start (the ADR 0018 recovery path); disk-full and write-error injection
at the storage seam (the `FlakyStore` pattern, promoted from the hub's test module to a
shared harness fixture); and brownout entry/exit (ADR 0041) mid-workload. The invariants
already say what must hold afterwards — acked data present, fencing intact, recovery
honest, nothing resurrected.

### 5. Two profiles: a CI gate and a soak

- **CI profile:** a bounded seed count and wall-clock budget per scenario, sized to keep
  the suite's total runtime acceptable — runs on every push like the rest of the suite
  (the ADR 0024 CI-gating discipline). Deterministic-core scenarios are cheap and run
  many seeds; whole-node stress runs few.
- **Soak profile:** the same scenarios, opted into more seeds / longer schedules via env
  (`MQTTD_SIM_SEEDS`, `MQTTD_SIM_MINUTES`-style knobs), for nightly or pre-release runs.
  Depth costs time; the knob makes the trade explicit instead of bloating every push.

### 6. Exhibits: flakes become tracked inputs

Known load-dependent flakes are the harness's first test cases, not noise: each gets an
entry in the delivery doc's exhibit ledger — reproduced under the harness (then fixed with
the seed as the regression test), or explained and recorded if the class is out of reach.
The ledger opens with the `a_takeover_recovers_the_retained_value_and_its_token` exhibit
from ADR 0041's T1 evidence. A future flake's triage starts by adding it here.

## Consequences

- **Good:** the durable plane's guarantees exist as one executable catalog instead of
  folklore; the ADR 0024 deferral is paid off for the layer that most needed it; fault
  *schedules* (not just single scripted faults) run on every push; flakes get a triage
  path with seeds instead of reruns; the first release ships with systematic evidence
  behind its durability claims.
- **Cost:** harness code is real code to maintain (schedule generator, fault injectors,
  checkers); CI minutes for the stress profile; the pure-core simulation constrains
  refactors that would push I/O into today's pure state machines (that constraint is
  a feature — the seam is the testability).
- **Risk:** stress tests that are themselves flaky would poison trust in the suite.
  Mitigations: the deterministic core is by construction not flaky; the whole-node layer
  asserts only *post-quiesce* invariants (never mid-schedule timing), sizes its CI profile
  conservatively, and a failure always carries its seed and trace — a harness failure is
  designed to be *more* debuggable than the flakes it replaces, or it has failed its
  own purpose.

## Alternatives considered

- **FoundationDB-style whole-process deterministic simulation** (own the runtime, the
  network, the disk; every run a pure function of a seed). The gold standard, and the
  reason §2 exists — but retrofitting it under tokio + openraft + redb is a rewrite of the
  I/O layer, not a pre-release task. The affordable slice is deterministic simulation of
  the pure core plus honest seeded stress around the rest; the seams cut for §2 are the
  path to widening determinism later. Rejected at whole-process scope, adopted at core
  scope.
- **Jepsen (or an external chaos framework) against real deployments.** Proven approach,
  wrong cost point: multi-machine orchestration, a new toolchain, and cross-process fault
  injection to reach the same fault classes in-process injection reaches today. Becomes
  attractive once multi-machine soak against released builds matters; deferred with a
  recorded path.
- **A property-testing library (proptest/quickcheck) instead of hand-rolled seeds.**
  Shrinking is attractive, but schedules shrink poorly (removing an event changes every
  subsequent interleaving), the codebase already has a working seed idiom (`swim_sim`,
  same xorshift style), and zero new dependencies is itself a security posture
  (ADR 0022's dependency discipline). Rejected for consistency; nothing precludes it
  later for value-shaped (non-schedule) properties.
- **Fix flakes one at a time as they surface.** The status quo. Each flake costs a
  debugging session under CI-only conditions, proves only that one symptom, and leaves
  the class untested. The exhibit ledger (§6) keeps the per-flake work, but inside a
  harness that turns a symptom into a seed. Rejected as the whole answer.
- **Simulate openraft's internals too.** It ships its own test suite and was adopted as
  the ratified engine precisely to not re-verify consensus (ADR 0006); simulating it
  re-opens that decision and couples the harness to its internals. Our glue — fencing,
  application, replay — is where our bugs live and is what §2 covers. Rejected.
