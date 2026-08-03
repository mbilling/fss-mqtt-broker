# ADR 0051 — Evaluation readiness: an assessable, comparable, migratable first release

- **Status:** Proposed
- **Date:** 2026-07-27
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0051-evaluation-readiness.md](../delivery/0051-evaluation-readiness.md) — plan, progress, and changelog
- **Related:** [ADR 0045](0045-release-engineering-and-distribution.md) (the release pipeline
  and the `0.x`-first plan this record sequences), [ADR 0038](0038-prerelease-compatibility-freeze.md)
  / [ADR 0039](0039-versioning-and-upgrade-policy.md) (what the `1.0.0` tag *means* — the freeze
  and the policy that starts at it), [ADR 0044](0044-release-readiness-assurance.md) (the
  engineering readiness checklist, green), [ADR 0048](0048-comparative-benchmarking.md) (the
  benchmark honesty rules this record extends to comparison prose, and the competitor set it
  widens), [ADR 0025](0025-boundary-mqtt-bridge.md) (the finished bridge this record makes
  visible), [ADR 0046](0046-file-based-configuration.md) (the config schema migration converges
  on), [ADR 0034](0034-foreign-client-interop-conformance.md) (the quickstart-as-test pattern
  the secured quickstart reuses)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0051-evaluation-readiness.md).

## Context

The engineering no longer blocks a release. ADR 0044's readiness checklist is green (the one
open box, 0039-T3 adjacent-release skew, is definitionally impossible before two releases
exist); ADR 0045's signed/reproducible/SBOM pipeline is built and waiting on its first tag;
the workspace sits at `0.9.0` with a `v0.9.0-rc` tag already cut.

A release-readiness review (2026-07-27) therefore assessed the repository the way its first
users will: as an **evaluator** — a newcomer deciding whether to look closer, and an expert
Mosquitto/EMQX/NanoMQ/VerneMQ operator deciding whether to switch. That lens found the gap
is not the engineering but the **evaluation experience**:

- **The finished bridge is invisible.** `mqtt-bridge` (ADR 0025, 11/11 tasks, standalone
  binary, deny-by-default directional rules, loop prevention, spool, HA) appears **zero
  times** in the README — not even in the workspace crate table. An evaluator checking for
  Mosquitto's flagship feature concludes it does not exist.
- **Comparison is left as an exercise.** There is no document mapping this broker against
  the ones evaluators already know. Deliberate absences (no dashboard, no HTTP admin API,
  no rule engine, no `$SYS`, no MQTT-SN) are recorded decisions — but nowhere *announced*
  as decisions, so principled omissions read as ignorance or concealment.
- **The 5-minute experience is the insecure mode.** Both copy-paste quickstarts use the
  plaintext listener. For a broker whose first principle is "security is the product",
  there is no copy-paste path to a secured node.
- **No migration path.** An operator with a working `mosquitto.conf`, an EMQX config, or a
  NanoMQ config must re-derive every setting by hand from the reference tables.
- **The headline claims lack public evidence.** Comparative bench results exist
  (0048-T1/T2, dev-grade) but nothing is published; "linear horizontal scalability" is in
  the tagline while the scaling curve (0048-T3) is still planned.
- **Docs drift with no guard.** The README says "44 ADRs" while 50 exist — the same
  staleness failure the ADR catalogue already solved by generating and CI-checking it.
- **Standard OSS surface is missing.** No `CONTRIBUTING.md` (only the agent-workflow one),
  no `CODE_OF_CONDUCT.md`, no issue/PR templates, no changelog policy, and no public MSRV /
  supported-platform / pre-1.0-stability statement.

Underneath all of it: the project has **no user base yet**, so credibility cannot come from
testimonials. It must come from what the repo already practices — verifiable artifacts
(reproducible builds, signatures, SBOM), disclosed limitations, and claims with evidence.
The persuasion assets exist; they are not assembled into an argument an evaluator can walk.

## Decision

**Evaluation readiness is a deliverable, and it gates `1.0.0` — not `0.9.0`.** Six parts:

### 1. Ship `v0.9.0` now; `1.0.0` stays a conscious freeze

Per ADR 0045's plan of record, a `0.x` release ships **first**; the tag that becomes
`1.0.0` is the ADR 0038 wire/schema freeze — a reviewed act after a bake window, once a
second tag makes the 0039-T3 skew test real. Jumping straight to `1.0.0` as the first-ever
release would freeze the wire with zero released-version soak and is rejected.

`v0.9.0` is gated only on fixing what **actively misleads** (the invisible bridge, stale
facts, the missing MSRV/platform/stability statements) plus the minimal community surface.
The rest of the evaluation package lands during the 0.9.x bake. Releases are cheap now;
every week at "no releases" costs more credibility than any missing polish.

### 2. The named comparison set: Mosquitto, EMQX, NanoMQ, VerneMQ

One honest comparison (`docs/COMPARISON.md`, condensed into a README matrix) against the
brokers evaluators actually arrive from: **Mosquitto** (ubiquity), **EMQX** (the
clustered incumbent), **NanoMQ** (the lightweight edge sibling), **VerneMQ** (the
architectural cousin — the only other masterless-clustered open-source broker, and
therefore the most informative head-to-head for the clustering and durability claims).
This widens ADR 0048's set — its alternatives said "widen if there is demand", and
maintainer demand has arrived, twice — so both join the **benchmark harness too**, not
just the prose (amended in ADR 0048, which also records the VerneMQ fairness terms:
non-comparable durability defaults disclosed, partition regimes stated, EULA'd test
images pinned).

Every cell is classified honestly: **matched**, **exceeded**, **missing by design** (with
the ADR that decided it), or **missing for now** (with the tracking task). ADR 0048's
honesty rules apply to comparison prose exactly as to numbers: competitor versions pinned,
claims dated, losing dimensions stated as prominently as winning ones.

### 3. Migration is tooling, not prose

Each comparison target gets a migration guide **and a small converter script**
(`scripts/migrate/from-{mosquitto,emqx,nanomq}.py` — Python 3, already a repo tool
dependency; zero new Rust dependencies). The scripts convert what is mappable
(listeners, TLS material paths, auth modes, ACLs, bridge topology → `mqtt-bridge` rules)
into an ADR 0046 TOML config, under three rules that mirror the broker's own posture:

- **Nothing is silently dropped.** Every unmappable directive is listed in a loud
  end-of-run report with *why* (no equivalent / by-design absence / needs a human).
  A migration that cannot be expressed is reported, never swallowed.
- **Secrets are never transformed.** Password hashes cannot be converted between schemes
  (Mosquitto's PBKDF2 is not Argon2id); the script says so and points at re-enrollment
  rather than pretending.
- **The output must validate.** A generated config that fails `mqttd --check-config`
  fails the script. The converter's contract is the same validate-before-swap bar as the
  broker's own reload.

The scripts are best-effort by declared design — a bounded common-subset mapping with an
honest failure mode, not a compatibility promise.

### 4. The secure path is the first path

A **secured quickstart** — generate certs, run TLS + mTLS + a real ACL, connect a foreign
client — stands beside the plaintext one as a copy-paste block, and gets the same
quickstart-as-test treatment (ADR 0034 / 0044 P7): CI runs the README's own commands, so
the secure path can never rot. The plaintext quickstart stays, clearly framed as the
local-loop exception.

### 5. Docs get CI guards where they have drifted

Derivable facts in the README (the ADR count today; others as they appear) are checked in
CI, the same pattern as the generated ADR catalogue and the config-table mapping test.
A fact that can drift, will; a fact that is checked, cannot.

### 6. The standard community surface exists

`CONTRIBUTING.md` (human-facing; the agent workflow doc remains separate),
`CODE_OF_CONDUCT.md`, issue/PR templates, and a stated changelog policy — GitHub Releases
(generated by the ADR 0045 pipeline) are canonical; a `CHANGELOG.md` pointer says so.
README states MSRV (1.85), supported platforms (linux/amd64 + linux/arm64 released
artifacts; macOS as a development platform with known test caveats), and pre-1.0 stability
semantics (ADR 0039 applies from 1.0.0; ADR 0038's freeze regime until then).

## Consequences

- The bridge, the comparison, the migration path, and the secure quickstart become part of
  the product surface — reviewed, tested, and release-gated like code. Docs that mislead
  are treated as defects, not cosmetics.
- Comparison and migration content create a **maintenance surface tied to competitors'
  formats and versions**. Bounded deliberately: versions pinned and claims dated (stale
  comparison is worse than none), re-checked per release alongside ADR 0048's cross-broker
  re-run; converters cover a declared common subset with a loud unmapped report.
- `v0.9.0` ships with known evaluation gaps (comparison, migration, published numbers land
  during the bake) — accepted, because the alternative is compounding the "no releases"
  credibility cost while polishing.
- The `1.0.0` gate gains explicit, checkable content beyond ADR 0038's freeze: the
  evaluation package delivered, a bake window survived, and the skew test real.
- Publishing an honest gap list (dashboard, rule engine, `$SYS`, MQTT-SN as by-design or
  not-yet) hands competitors a feature checklist — accepted; a security-first brand cannot
  simultaneously argue "trust our disclosures" and hide its own gaps.

## Alternatives considered

- **Jump straight to `1.0.0`:** contradicts ADR 0045's recorded `0.x`-first plan; freezes
  wire/schema with zero released-version bake; makes the first-ever release also the
  hardest-to-revise one. Rejected.
- **Feature parity before releasing (dashboard, HTTP admin API, rule engine):** open-ended
  scope, and partly *contradicts recorded decisions* — the read-only, unauthenticated
  health listener and signal-driven operator control are deliberate (README, ADR 0032/0040
  posture). The comparison matrix discloses these as by-design; parity is not the bar.
  Rejected.
- **Let the docs grow organically:** the demonstrated failure mode — a finished flagship
  feature invisible in the README and a hand-written count three ADRs stale (twice: the
  ADR catalogue drifted the same way before it was generated). Rejected.
- **Guides without converter scripts:** pushes the same mapping cost onto every single
  evaluator, precisely where switching friction decides the outcome. The scripts are small,
  testable against fixtures, and honest about their limits. Rejected as the default (guides
  alone remain the fallback for corners the scripts decline).
- **A broader comparison set (HiveMQ, rumqttd, …):** more maintenance for brokers
  evaluators arrive from less often; ADR 0048's diminishing-returns argument stands. The
  set has widened exactly where demand existed — NanoMQ (2026-07-27), then VerneMQ
  (2026-08-03, after the architecture analysis made its head-to-head value concrete);
  further only on further demand.
