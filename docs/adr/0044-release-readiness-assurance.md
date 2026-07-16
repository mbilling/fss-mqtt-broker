# ADR 0044 — Release readiness: out-of-process cluster harness and continuous assurance

- **Status:** Proposed
- **Date:** 2026-07-15
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0044-release-readiness-assurance.md](../delivery/0044-release-readiness-assurance.md) — plan, progress, and changelog
- **Related:** [ADR 0042](0042-durable-plane-stress-harness.md) (the in-process stress
  harness and acked-facts oracle this ADR lifts to real processes),
  [ADR 0043](0043-elastic-cluster-resize.md) (resize vocabulary; recorded the
  two-binary rolling-upgrade gap), [ADR 0039](0039-versioning-and-upgrade-policy.md)
  (the upgrade promise this ADR makes testable; T3 rides P3's machinery),
  [ADR 0038](0038-prerelease-compatibility-freeze.md) (wire/schema freeze — the
  disk-reopen and skew tests exercise its gates), [ADR 0024](0024-deterministic-testing.md)
  (determinism posture; the out-of-process tier trades some of it for realism, deliberately),
  [ADR 0034](0034-foreign-client-interop-conformance.md) (interop harness; T7's second
  client lands here), [ADR 0018](0018-on-disk-persistence.md) (T7's SIGKILL
  crash-consistency test lands here), [ADR 0007](0007-durable-store-integration.md)
  (T8's flap-stress lands here), [ADR 0041](0041-resource-governance.md) (the caps and
  watermarks the soak tier holds against drift), [ADR 0020](0020-metrics-and-observability.md)
  (the gauges the soak tier watches)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0044-release-readiness-assurance.md).

## Context

The project is approaching its release commitment: supported at a high level, aimed at
enterprises and small businesses alike, with two standing product claims — **the most
secure MQTT broker in the field, continuously**, and **the simplest to operate**
([CAPABILITY-PLAN](../CAPABILITY-PLAN.md)). A release is a promise about behaviour under
conditions we did not stage; the remaining work before making that promise is assurance,
not features.

The assurance inventory today is strong but has a structural ceiling:

- The seeded stress harness (ADR 0042/0043) — kill, restart, disk faults, brownouts,
  join, decommission under the acked-facts oracle — runs **in one process sharing one
  binary**. Everything it cannot represent is exactly what is deferred: the two-binary
  rolling upgrade (0039-T3 and the ADR 0043 recorded gap), true `SIGKILL`
  crash-consistency (0018-T7), OS-real partitions, and rapid-churn flap stress (0007-T8).
- **Fuzzing** exists as a single target (`mqtt-codec` packet decode) that CI never runs.
  The attack surface is every byte parser: MQTT packets, peer frames, gossip datagram
  verification, bridge frames, WebSocket/QUIC framing. "Most secure, continuously"
  requires the adversarial input generator to run continuously, not once at authoring.
- **No benchmarks exist** — zero measured throughput/latency numbers and no regression
  gate. Top-tier is a measurable claim; enterprises will benchmark us against
  incumbents on day one, and a PR can silently regress the hot path today.
- **Soak is absent.** Nothing runs for hours; memory/FD/latency drift — where
  enterprise-grade rot lives — is invisible to a CI suite measured in minutes.
- **Interop has one oracle** (mosquitto; 0034-T7 deferred the second), and the
  operator quickstart is prose that nothing executes — the "simplest to use" claim is
  untested by construction.

## Decision

Assurance becomes the product until release. One spine — an **out-of-process cluster
harness** — plus the continuous programs that stand on it. Seven parts:

### 1. The out-of-process harness is first-class test infrastructure

A harness that spawns **real `mqttd` processes** (the compiled binary via Cargo's test
binary paths) with real data directories, real TCP/TLS listeners, and real gossip
sockets, driven by real MQTT clients — and ports the ADR 0042 schedule vocabulary and
**acked-facts oracle** unchanged: every acknowledged fact must survive whatever the
schedule did. The oracle stays the single source of truth; the in-process harness
remains for fast, deterministic per-PR coverage. Link-level faults (partition, latency,
loss, half-open) are injected by **unprivileged per-link TCP relays** — the pattern the
in-process harness already proved — so the whole tier runs on stock CI runners;
privileged `netem` shaping is an optional local extension, never a CI dependency.

### 2. Faults become OS-real

The vocabulary the harness gains is exactly what one process cannot fake: `SIGKILL` at
any instant (including mid-fsync — 0018-T7's crash-consistency claim moves from
"rests on redb's test suite" to demonstrated on our own data), disk-full against a real
filesystem bound, restart from surviving data dirs, and membership flap at
SWIM-confusing rates (0007-T8). Crash semantics are no longer simulated; they are
delivered by the kernel.

### 3. The rolling upgrade is proven with two binaries

The harness builds **two** broker binaries — HEAD and a designated baseline (pre-1.0: a
pinned earlier ref; post-1.0: the previous release) — and rolls a live cluster one node
at a time in both directions under the oracle, including reopening each node's data
dirs across versions (the ADR 0038 schema gates fire for real, not in a unit test).
This closes the ADR 0043 recorded gap and builds the machine 0039-T3 rides: when the
first post-1.0 release exists, the CI adjacent-pair skew test is this test pointed at
two release tags.

### 4. CI becomes tiered: fast on every PR, deep every night

Per-PR CI stays as it is (fast suite, in-process harness, interop, audit). A scheduled
**nightly tier** runs what minutes cannot: the out-of-process schedules across a wide
seed sweep, the two-binary upgrade paths, fuzzing time, and a **soak run** — hours of
sustained mixed load watching RSS, file descriptors, and tail latency against declared
drift watermarks (the ADR 0041 caps and ADR 0020 gauges make "no drift" checkable). A
nightly failure is triaged with the same exhibit-ledger discipline as ADR 0042.

### 5. Security assurance runs continuously

Every parser that consumes attacker-reachable bytes gets a fuzz target with an in-repo
corpus: MQTT packet codec (exists), peer-frame decode, gossip datagram verification,
bridge frames, WebSocket/QUIC framing, and the auth/config parsers. Fuzzing runs in the
nightly tier with corpora persisted so coverage accumulates; every fuzz find lands as a
regression test (darksky grows from the findings). The supply-chain audit stays per-PR.
A **security response process** is documented (SECURITY.md: private reporting channel,
triage bounds, advisory + patched-release path) — enterprises evaluate the process as
much as the code.

### 6. Performance is measured, baselined, and gated

Criterion micro-benchmarks for the hot paths (codec encode/decode, hub fan-out,
replica apply/group-commit) and a harness-driven macro benchmark (connection ramp,
sustained msgs/sec, p99 end-to-end at durable QoS 1) with **recorded baselines** in the
repo. The nightly tier compares against baseline and flags regressions beyond a stated
tolerance; the numbers become the honest core of any "top tier" statement.

### 7. Conformance and operator experience widen

A second foreign client (Paho, per 0034-T7) joins mosquitto behind the same interop
harness with richer assertions (reason codes, properties, flow control). And the
"simplest to use" claim becomes executable: a smoke test stands up the documented
quickstart — a 3-node cluster from nothing but the README's own commands — so the
operator path can never silently rot. Release readiness is a checklist assembled from
parts 1–7, and 1.0 ships only when it holds.

## Consequences

- The release gate is now defined and mechanical: the oracle holds across real
  processes, real crashes, real partitions, a real two-binary rolling upgrade, a soak
  run without drift, fuzzers finding nothing new, benchmarks at baseline, both interop
  oracles green, and the quickstart executing verbatim.
- Four deferred items gain their missing prerequisite and un-defer into this ADR's
  delivery: 0018-T7, 0007-T8, 0034-T7, and the ADR 0043 rolling-upgrade gap
  (0039-T3 itself still waits for two releases to exist, by definition).
- CI cost grows deliberately: the nightly tier buys depth with scheduled minutes
  instead of slowing every PR. Corpora and baselines live in-repo and need occasional
  curation.
- The out-of-process tier is less deterministic than ADR 0024's in-process discipline —
  accepted: it exists precisely to cover what determinism cannot reach, and every
  schedule stays seeded and logged for best-effort reproduction.
- No license gate ships: the same binary serves commercial and non-commercial use.

## Alternatives considered

- **Container/orchestrator-based harness** (testcontainers, kind/k8s): heavier, slower,
  and adds a runtime dependency between the tests and the thing tested; plain processes
  with per-link relays cover the fault space with none of the moving parts. Revisit for
  k8s-specific operator docs testing.
- **External model checker (Jepsen-style)**: the acked-facts oracle already encodes our
  consistency claims in-repo and has a two-ADR track record of finding real bugs;
  porting it beats re-deriving it in another language and keeps one source of truth.
- **Privileged network faults only (netns/tc-netem)**: more physically real, but
  requires root and forks the harness between CI and local runs; unprivileged relays
  run everywhere and already proved the pattern in-process. netem remains a local
  option.
- **Commercial fuzzing service / OSS-Fuzz first**: worth pursuing later; an in-repo
  cargo-fuzz nightly with persisted corpora delivers most of the value now with zero
  external coupling.
- **Ship features until 1.0 instead** (status quo): every remaining product claim is an
  assurance claim; more features widen the surface this ADR must then cover.
