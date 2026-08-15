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
(`scripts/migrate/from-{mosquitto,emqx,hivemq,nanomq}.py` — Python 3, already a repo tool
dependency; zero new Rust dependencies). The scripts convert what is mappable
(listeners, TLS material paths, auth modes, ACLs, bridge topology → `mqtt-bridge` rules)
into an ADR 0046 TOML config, under three rules that mirror the broker's own posture:

- **Nothing a converter READS is silently dropped.** Every unmappable directive it reads is
  listed in a loud end-of-run report with *why* (no equivalent / by-design absence / needs a
  human). A migration that cannot be expressed is reported, never swallowed. The qualifier is
  load-bearing and is stated in rule 8 below: a construct never seen cannot be reported, and a
  construct read but MISUNDERSTOOD is reported as translated.
- **Secrets are never transformed.** Password hashes cannot be converted between schemes
  (Mosquitto's PBKDF2 is not Argon2id); the script says so and points at re-enrollment
  rather than pretending.
- **The output must validate.** A generated config that fails `mqttd --check-config`
  fails the script. The converter's contract is the same validate-before-swap bar as the
  broker's own reload.

The scripts are best-effort by declared design — a bounded common-subset mapping with an
honest failure mode, not a compatibility promise.

**Amended 2026-08-14 (issue #250, T7/T17/T18).** Two changes to this section, both from
the 2026-08-13 review panel, where migration tooling was the top non-benchmark adoption
blocker for two reviewers whose brokers had no converter:

1. **HiveMQ joins the converter set.** The list above named
   `from-{mosquitto,emqx,nanomq}.py`; HiveMQ appeared in this ADR only as an evaluator's
   broker. It is now `from-{mosquitto,emqx,hivemq,nanomq}.py`.
2. **A converter is not enough on its own, so the guide is now a named deliverable.**
   mqttd cannot import another broker's session state, so a config converter leaves the
   operator with the harder half of the problem — moving live traffic. The migration guide
   promised per target is therefore consolidated into one document,
   [`docs/MIGRATION.md`](../MIGRATION.md), which carries the per-broker mapping tables
   **and** a dual-run cutover playbook written against what `mqtt-bridge` actually
   supports (ADR 0025): bridge both brokers, move clients in cohorts, verify, cut, with
   rollback being "re-widen the rule, the incumbent is still live".

A fourth rule, learned from the 2026-08-11 panel finding that mapping Mosquitto's
`cafile` onto `client_ca` silently converted cert-optional TLS into mandatory mTLS
(#162), is now stated explicitly rather than left as a bug fix: **a mapping that changes
security posture is not a mapping.** Where a source setting is permissive and its nearest
mqttd equivalent is mandatory (or the reverse), the converter emits the candidate line
**commented out** beside a `TODO(migrate)` explaining the choice, and never picks for the
operator. The same rule governs ACL translation: an EMQX condition or a HiveMQ permission
qualifier that mqttd cannot express means the affected rule is **not emitted**, because a
rule that silently widens or narrows a policy is worse than a reported gap.

**Amended 2026-08-14, second pass on the same issue (#250).** Rule 4 needed one clause and
a fifth rule needed writing down, both learned from an independent verification of the EMQX
and HiveMQ converters that found the *same* defect in both, in the fail-open direction:

- **Rule 4 is decided across every listener that shares the target setting, not per
  listener.** mqttd has one `[tls]` table serving `tls_bind`, `wss_bind` *and* `quic_bind`,
  so a source broker's per-listener mTLS posture collapses onto a single node-wide gate.
  Reading the posture off the *first* TLS listener in document order — which both
  converters did — silently discards a mandate on any other one. A posture mapping is
  therefore a decision over the **set**: unanimous, map it; **mixed**, emit the candidate
  commented with a TODO naming which listeners demanded certificates and which did not.
  Any per-listener setting that cannot be expressed in the single-bind-per-protocol shape
  must appear as a TODO naming the listener it came from.
- **Rule 5: an equivalence must be same-direction.** Two settings with similar names are
  not a mapping if they bound opposite flows. EMQX's `mqtt.max_inflight` (broker→client) is
  not mqttd's `[limits] receive_maximum` (the inbound window granted to clients), and
  mapping them would have cut every stock conversion's window from 256 to 32. Where the
  nearest-looking equivalent runs the other way, the converter states the flip in a TODO
  and offers a commented candidate — it does not present it as an equivalence.

**Amended 2026-08-15, third pass on the same issue (#250), and this amendment is about the
*shape* of the guard rather than a new mapping rule.** Two adversarial rounds each fixed the
named sites of a defect and left the class alive elsewhere — round 2's blocking finding was
round 1's blocking finding, surviving in the third converter because nobody was told to look
at it. Rules 1–5 were all in force and all three converters still shipped instances of them.
The reason is structural: the fixture tests are **example-based**, one input each and a list
of greps, so they can only see a defect at the place a reviewer already looked. Each
converter's harness had only ever fed it *one ordering of one listener set*, which is exactly
why the same first-listener defect could hide three times.

- **Rule 6: every rule above is enforced over GENERATED inputs, not only over fixtures.**
  `scripts/migrate/property_sweep.py` builds each converter's inputs from a cross product —
  listener **order** permutations, `enable` flags, unanimous and mixed mTLS postures, both
  `no_match` postures, truststore present and absent — and asserts one invariant per defect
  class on every case: every security-relevant input **value** appears in the output
  (translated or named in a `TODO`/`NOTE`); nothing the source **disabled** is a live bind,
  URL or rule; no `deny`/`allow` claim contradicts the `default` the same document writes;
  every numbered step the output cites is a step the output printed; and `mqttd
  --check-config` accepts every generated config. All three `test-from-*.sh` scripts run it,
  so it is CI-gated, and each invariant is mutation-proved. A fixture test pins provenance and
  exact wording, which a property test cannot; a property test finds the instance nobody
  thought to write a fixture for. Both, or neither is trustworthy.
- **A corollary that cost this lane a whole round: a sentence about what the output WILL DO
  must be derived from the value being emitted.** Both zero-rule ACL TODOs asserted
  "fail-closed … `default = "deny"`" as a constant while the renderer wrote
  `authorization.no_match`, so a wide-open policy could carry a comment saying it denied
  everything. Prose that states a computed outcome is code, and is written as code.

**Amended 2026-08-15, fourth and fifth passes on the same issue (#250).** Rules 1-6 were all in
force and the finding count still went up, so two more rules — the first about the *mechanism*,
the second about what no mechanism can reach.

- **Rule 7: PROVENANCE OR NOTHING. A security-relevant value is emitted through one gate that
  takes the value AND the input key it came from, and refuses to write a live line without one.**
  Every finding of rounds 1-3 that mattered was the same shape — a live setting the tool had not
  derived from the input (a bind fabricated as `0.0.0.0:1883`, a mandate taken from the wrong
  listener, `allow_anonymous true` carried off a retired listener). Fixing those one at a time is
  unbounded work, because the set of vendor constructs nobody has looked at is unbounded; so the
  shape is made impossible instead. `SECURITY_FIELDS` names the fields whose value decides who may
  connect and what they may do, `Provenance.line`/`Conversion.set` is the only way to write one,
  and a field with no source comes out commented beside a TODO naming the decision. Every live
  security-relevant line therefore carries `# from: <input key>`, which invariants **F** and **G**
  of the property sweep check on the output, and **H** extends to the one property `--check-config`
  cannot see: that a live bind is an address the broker can actually bind.
- **Rule 8, and it is a LIMIT rather than a guarantee: the gate closes FABRICATION, not
  MISREADING, and the difference is stated wherever the gate is claimed.** A value derived from a
  real input key whose *meaning* the converter got wrong is live, honest-looking and wrong: a
  Mosquitto TLS-PSK listener emitted as a plaintext bind (the field encodes the transport; the
  gate only validates the value), an ACL block the vendor scopes to anonymous clients emitted as a
  grant to everyone, `message_size_limit 0` — the vendor's spelling of *no limit* — emitted as a
  1 KiB ceiling. No invariant over the output can see any of them, because the output is
  consistent with the input. So rule 1 is scoped to **every construct a converter READS**, and
  every construct known to be misread or unhandled is enumerated in `docs/MIGRATION.md`'s KNOWN
  GAPS table with what the operator must check by hand. A gap in a table beats a promise that does
  not hold: the alternative — implying the class is closed because the mechanism is sound — is the
  claim this ADR's whole migration section rests on not making.

Honesty limit recorded here because it bounds what these scripts can claim: the EMQX and
HiveMQ converters were built from each vendor's own shipped example configuration at a
pinned tag, and **no live EMQX or HiveMQ broker was ever run**. The Mosquitto converter has
no vendor fixture at all — its mappings are derived from `mosquitto.conf(5)` at a pinned tag,
which is weaker still, and its `--help` says so. The fixtures, all three converters'
`--help`, `mqttui --help`, and `docs/MIGRATION.md` state the same version scope in the same
words, because a caveat that lives only in a document is a caveat a hurried operator skips.

One further honesty rule about the fixtures themselves, added 2026-08-14 after a
verification pass found a fixture header claiming "fetched verbatim … every KEY and VALUE
as the vendor ships it" for a file with nine changed values: **a fixture is either verbatim
or composed, it says which in its own header, and a composed one lists every deviation from
the vendor's file and why that deviation exists.** Both kinds are needed and neither is
second-class — a vendor's stock defaults take the *refusing* branch of nearly every security
mapping, so verbatim fixtures prove the refusals while composed ones prove the mappings —
but calling the second kind the first is the class of claim this ADR's whole migration
section rests on not doing. `docs/MIGRATION.md` states which fixtures are which and what
each one proves.

And one rule about the *other* half of a migration document, learned the same day from the
same review: **a claim about an observable — a metric series, an exit code, a log line — is
scraped or run before it is written down, and named exactly as the binary emits it.** The
playbook had told operators to alert on five bridge metrics that do not exist (the real
series carry the `fss_` registry prefix, and broker series carry `mqttd_` plus a counter's
`_total`), so the two signals that matter most during a cutover would have been silently
unmonitored while the dashboard looked healthy. A converter's own output is subject to the
same rule: it cites metric names too. Where a harness asserts such a name it must match it
**anchored**, because a substring match on `bridge_` is what let the wrong names survive.

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
README states MSRV (**1.88**, and 1.89 for `mqttd-operator` — this ADR originally said
1.85, which was already stale when written; the declared `rust-version` is the source of
truth and is now verified by the nightly `msrv` job rather than asserted), supported
platforms (linux/amd64 + linux/arm64 released
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
