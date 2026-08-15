# ADR 0044 — Release readiness: out-of-process cluster harness and continuous assurance

- **Status:** Accepted
- **Date:** 2026-07-15 (accepted 2026-07-17 — P1–P7 delivered; see the delivery doc)
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

> **As delivered (issue #260, 0044-P11).** The tiers were the right shape and had a hole
> underneath them: *a green tier and a tier that did not run look identical*. Two mechanisms
> produced that, and neither showed up in a `cargo test` result — a test returning early
> because the environment was missing something (a live one: `mqttui`'s only
> signature-refusal test self-skipped on **every** CI run, because no job installs `cosign`),
> and a wall-clock `sleep` standing in for a condition. Both are now build failures via
> `scripts/check-test-hygiene.py`, a third repo gate beside `gen-status.py` and
> `check-readme-facts.py` in the `docs` job. A skip is allowed locally and asserts under
> `CI=true`; every wait in test code must be a bounded poll, a `start_paused` virtual-clock
> advance, or a settling delay whose reason is written at the site and listed in
> `docs/test-settling-delays.md`. The taxonomy and the bar a settling delay must clear live in
> [TEST-PLAN § Conventions](../TEST-PLAN.md#conventions).
>
> **As delivered (2026-08-15, second pass).** The gate was then attacked, and eleven working
> bypasses were found and closed — including one that made its own CI-fatality check vacuous
> (it searched raw text, so a comment quoting the deleted assertion satisfied it). Two lessons
> are now structural rather than remembered: every check reads a comment- and string-stripped
> view of the source, and text rules are no longer the only mechanism. `cargo test -- --list`
> enumerates the tests each binary actually contains, compared in CI against a generated
> [test inventory](../test-inventory.md) — which catches a test `cfg`-gated out of existence, a
> file that compiled to zero tests, and a silent deletion, none of which a rule over source
> text can see reliably.
>
> **As delivered (2026-08-15, third pass).** Attacked again; eight more bypasses closed, and two
> of them turned out to be one missing mechanism rather than two missed patterns. `#[ignore]`
> takes a test out of every run while the binary still *lists* it, and a single
> `std::process::exit(0)` inside one test discards **every** result in its binary — `running 6
> tests`, then no per-test lines and no summary, with `cargo test` exiting 0. Neither is visible
> to any rule over source text, and both are plain in the run's own output, so the run's own
> output is now checked (`--check-results`): per binary, a complete summary, no failures, nothing
> filtered out, the passed count the inventory accounts for on this host, and an ignored set that
> matches an allowlist whose declared tier is *verified* against `.github/workflows/`. It reads
> the log the test job already tees, so it costs the job nothing. The first thing it surfaced is
> a real gap rather than a hypothetical one: five `durable_bench` benchmarks are `#[ignore]`d and
> run by **no** tier — per-PR, nightly or release — which is coverage that exists only on paper,
> now declared and printed rather than invisible.
>
> What the gate detects and what it cannot is a two-part table in
> [TEST-PLAN § What this gate detects, and what it cannot](../TEST-PLAN.md#what-this-gate-detects-and-what-it-cannot):
> every "detected" row was proven by reintroducing the bypass and watching the gate name it, and
> every "not detected" row by running that shape green. A limit that is not written down gets
> trusted past, and a claim that outruns its check is worse than the gap it hides — which is why
> the gate's success line now enumerates what it verified instead of asserting that no self-skip
> can pass.

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

## Release-readiness checklist

The 1.0 gate, assembled from parts 1–7. Each line is a green CI signal, not a
judgement call. All hold as of ADR acceptance (2026-07-17):

- [x] **Acked-facts oracle holds in-process** across the ADR 0042 seeded fault
      schedules (kill, restart, disk fault, brownout, join, decommission).
- [x] **Acked-facts oracle holds out-of-process** (P1/P2): real spawned binaries,
      kernel `SIGKILL` (incl. mid-write), `SIGXFSZ` disk-full, SWIM-rate flap,
      relay partitions/half-open/brownout links.
- [x] **Two-binary rolling upgrade + rollback** loses no acked fact, dirs reopened
      across versions (P3) — the ADR 0043 gap closed.
- [x] **Soak** shows no RSS/FD/tail-latency drift over an hour (P4).
- [x] **Every attacker-reachable parser is fuzzed** with a clean pass, and a
      `SECURITY.md` response process ships (P5).
- [x] **Benchmarks recorded and gated**: hot-path numbers in the baseline doc, a
      per-PR throughput floor, nightly comparison (P6).
- [x] **Two independent foreign-client oracles green** (Mosquitto CLI + Paho
      Python), and the **README quickstart executes verbatim** (P7).
- [x] **No test can report success without running** (P11, issue #260): an environmental
      self-skip is fatal under `CI`, a `#![cfg]`-gated suite that compiled to nothing is
      fatal under `CI`, every wall-clock wait in test code is a bounded poll, a
      virtual-clock advance, or a documented settling delay, and the tests that actually
      **ran and passed** are compared against a checked-in inventory — including the ones
      `#[ignore]` would have retired in silence. Checked by `scripts/check-test-hygiene.py`
      in the `docs` job (text rules), and by `--check-inventory` / `--check-results` in the
      jobs that build and run. Its known limits are tabled in TEST-PLAN; the open one worth
      naming here is five ignored `durable_bench` benchmarks that no tier runs.
- [ ] **Adjacent-release skew smoke in CI** (0039-T3): the machinery exists (P3);
      the test itself needs two released versions — impossible before 1.0 by
      definition. This is the one gate that opens *at* 1.0, not before.

When the first release ships, 0039-T3's box is checked by pointing P3's
rolling-upgrade test at two release tags; nothing else on the list moves.

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
