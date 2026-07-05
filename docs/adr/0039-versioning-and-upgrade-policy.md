# ADR 0039 — Release versioning and upgrade policy (semver, adjacent skew, sequential majors)

- **Status:** Accepted
- **Date:** 2026-07-05
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0039-versioning-and-upgrade-policy.md](../delivery/0039-versioning-and-upgrade-policy.md) — plan, progress, and changelog
- **Related:** [ADR 0038](0038-prerelease-compatibility-freeze.md) (the mechanisms this
  policy is enforced by: peer-proto negotiation and schema stamps),
  [ADR 0024](0024-deterministic-testing.md) (the testing posture the skew matrix rides on)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0039-versioning-and-upgrade-policy.md).

## Context

ADR 0038 is putting the *mechanisms* of evolvability in place before the first release:
a version-negotiating peer handshake and schema-stamped stores, both fail-closed. What
they enforce is still undecided — how versions are numbered, which version pairs may
coexist in one cluster, what an operator's upgrade path looks like, and which release
lines receive fixes. Deciding this *before* 1.0 costs a page of text; deciding it after
means retrofitting promises onto releases that never made them.

The proven models to draw from:

- **Kubernetes**: a version-skew policy (components tolerate N-2/N-3), sequential
  control-plane minor upgrades, and patches for the **3 most recent minor lines**.
  Proven at enormous scale — but the wide skew window is also an enormous test matrix.
- **etcd / Cassandra**: mixed versions allowed **only transiently during a rolling
  upgrade**, one version step at a time, no skipping. The narrowest — and therefore
  most testable — contract that still gives zero-downtime upgrades.
- **Kafka**: fully negotiated per-API version ranges; nearly any-to-any broker skew.
  The most flexible — at the cost of keeping every historical frame shape alive and
  tested indefinitely.
- **PostgreSQL**: minors are compatible; majors migrate offline via a tool (skipping
  allowed). Proven, but the offline step is the wrong shape for a clustered,
  rolling-upgrade system.

## Decision

**Semantic versioning with compatibility defined at the wire/disk layer; adjacent-only
version skew enforced by the peer handshake; sequential major upgrades enforced by the
schema gate; patches for the three most recent minor lines. Applies from 1.0.0 — until
then, ADR 0038's freeze-and-break regime stands.**

### 1. Semantic versioning, defined by what actually breaks

- **MAJOR** — any breaking change: a peer-bus frame shape (peer proto bump), a store
  layout (schema-version bump), or removed/changed config and client-visible behavior.
- **MINOR** — additive and fully compatible: new frames/fields behind the negotiated
  proto, new tables/columns readable by the previous minor, new config with safe
  defaults. A mixed cluster of adjacent minors must work — indefinitely, not just
  mid-roll.
- **PATCH** — fixes only. **No format changes of any kind.**

The semver label *communicates* compatibility; the peer proto range and schema stamps
*define* it. A release that touches neither is minor/patch by construction.

### 2. Version skew: adjacent releases only

A cluster may mix release N and N+1 (one step, major or minor) — the state every
rolling upgrade passes through, and the only mixed state that is **supported and
tested**. Enforcement is ADR 0038's handshake: each release sets `proto_min` to the
previous release's proto, so a two-step-apart pair has disjoint ranges and fails
closed at `Hello` with an error naming both.

Chosen over Kubernetes' wider window deliberately: every supported skew pair is test
matrix that must genuinely run (the durable plane's history shows untested pairs are
where latent bugs live). **Widening later is a compatible policy change; narrowing
never is** — so start at the provable minimum and widen when the stress/simulation
harness (pre-release area ④) can carry the load.

### 3. Major upgrades are sequential: 1 → 2 → 3

Each major ships migrations from **exactly one major back**: version N+1 reads/migrates
N's store layouts (dispatched on the schema stamp) and speaks N's peer proto during the
roll (`proto_min = N`). Skipping fails closed twice — at the handshake (disjoint proto)
and at the schema gate, whose error names the version to route through. This is the
etcd/Cassandra path; it keeps the migration surface exactly one transition wide.

### 4. Supported lines: the three most recent minors

Patches and security fixes land on the latest three minor lines (Kubernetes-style).
Older lines are EOL: upgrade to a supported line first. This is a maintenance promise,
independent of the skew rule — running 1.2 and 1.5 in one cluster is still unsupported
even while both receive patches.

### 5. What this policy covers — and what it doesn't

Covered: the cluster bus (peer frames, gossip postures), on-disk stores, configuration
surface, and operational behavior. **Not covered:** MQTT client compatibility, which
is governed by the MQTT specifications (3.1.1 / 5.0) — clients of any age keep
working; that is the protocol's promise, not this policy's.

## Consequences

- **Good:** operators get one memorable contract (adjacent rolls, sequential majors,
  three patched lines); the test matrix stays exactly as wide as what is promised; the
  enforcement is mechanical (handshake + gate), not documentation-only; the pre-1.0
  freeze regime has a defined end.
- **Cost:** clusters more than one step behind take multiple rolls to catch up; three
  patched lines means up to three release branches receiving backports.
- **Trade-off accepted:** less skew flexibility than Kubernetes/Kafka in exchange for
  a matrix a small project can actually test. The proto-range mechanism already
  supports widening per-release if that trade ever flips.

## Alternatives considered

- **Kubernetes-width skew (N-2/N-3).** Rejected *for now*: each extra supported pair
  is a real test surface; adopt by widening `proto_min` once the sim harness proves
  the pairs — a purely additive change.
- **Kafka-style any-to-any negotiation.** Rejected: keeping every historical frame
  alive indefinitely is the single most expensive compatibility posture; adjacent
  stepping bounds it to one version.
- **Postgres-style skip-major offline migration.** Rejected: an offline tool is the
  wrong shape for a clustered broker whose whole point is rolling everything.
- **CalVer.** Rejected: dates say when, not whether an upgrade is safe; semver's
  break/compatible signal is the one operators act on.
