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
- **Elasticsearch**: rolling into a new major is supported **only from the final minor
  of the previous major** (5.6 → 6.x, 6.8 → 7.x); that gateway release is where the
  deprecation checks and known upgrade fixes ship before the jump.
- **Kafka**: fully negotiated per-API version ranges; nearly any-to-any broker skew.
  The most flexible — at the cost of keeping every historical frame shape alive and
  tested indefinitely.
- **PostgreSQL**: minors are compatible; majors migrate offline via a tool (skipping
  allowed). Proven, but the offline step is the wrong shape for a clustered,
  rolling-upgrade system.

## Decision

**Semantic versioning with compatibility defined at the wire/disk layer; adjacent-only
version skew enforced by the peer handshake; sequential major upgrades that roll only
from a designated gateway minor of the previous major, enforced by the handshake and
the schema gate; patches for the three most recent minor lines. Applies from 1.0.0 —
until then, ADR 0038's freeze-and-break regime stands.**

### 1. Semantic versioning, defined by what actually breaks

- **MAJOR** — any breaking change: dropping support for an old peer proto (a
  `proto_min` raise), a store layout change (schema-version bump), or removed/changed
  config and client-visible behavior.
- **MINOR** — additive and fully compatible: new frames/fields may ship under a new
  negotiated proto (a `proto_max` bump) **so long as every proto back to the major's
  floor is still spoken in full**; new tables/columns readable by the previous minor;
  new config with safe defaults. A mixed cluster of adjacent minors must work —
  indefinitely, not just mid-roll.
- **PATCH** — fixes only. **No format changes of any kind.**

The semver label *communicates* compatibility; the peer proto range and schema stamps
*define* it: minors may raise `proto_max` (additive), only majors may raise
`proto_min` (breaking). A release that touches neither store layouts nor the proto
floor is minor/patch by construction.

### 2. Version skew: adjacent releases only

A cluster may mix release N and N+1 (one step, major or minor) — the state every
rolling upgrade passes through, and the only mixed state that is **supported and
tested**. Within a major the handshake is deliberately permissive (`proto_min` is
frozen for the major's lifetime, so any minor pair negotiates); the adjacent-only rule
there is a support-and-testing promise. Across a major boundary it is mechanical: the
gateway rule (§3) sets the new major's `proto_min` so that only the gateway minor's
range overlaps — anything older has a disjoint range and fails closed at `Hello` with
an error naming both.

Chosen over Kubernetes' wider window deliberately: every supported skew pair is test
matrix that must genuinely run (the durable plane's history shows untested pairs are
where latent bugs live). **Widening later is a compatible policy change; narrowing
never is** — so start at the provable minimum and widen when the stress/simulation
harness (pre-release area ④) can carry the load.

### 3. Major upgrades are sequential — and roll through a gateway minor

Rolling into major N+1 is supported **only from a designated gateway minor of major
N** — by default N's latest minor, pinned in N+1's release notes ("2.0 upgrades from
1.4 or later"). The gateway is where known upgrade issues are fixed *before* the jump
(deprecation checks, migration preconditions — the Elasticsearch 6.8 → 7.x model): an
operator first rolls to the gateway, an ordinary compatible minor roll, then rolls to
N+1.

Enforcement is twofold and fail-closed:

- **Handshake**: major N+1 sets `proto_min` to the gateway minor's proto, so a
  pre-gateway node's range is disjoint and the link is refused at `Hello`.
- **Schema gate**: each major ships migrations from **exactly one major back**
  (dispatched on the schema stamp); skipping a major fails at the gate, whose error
  names the version to route through.

Sequential majors (1 → 2 → 3, no skipping) keep the migration surface one transition
wide; the gateway requirement keeps the *tested* cross-major pair exactly one pair
wide — (gateway of N) ↔ N+1 — instead of one per from-minor.

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

- **Good:** operators get one memorable contract (adjacent rolls, majors via the
  gateway minor, three patched lines); the test matrix stays exactly as wide as what
  is promised — one cross-major pair per transition; known upgrade issues have a
  designated place to be fixed (the gateway) instead of a matrix of from-versions;
  the enforcement is mechanical (handshake + gate), not documentation-only; the
  pre-1.0 freeze regime has a defined end.
- **Cost:** a major upgrade is two rolls when the cluster is not already on the
  gateway minor (roll to gateway, then to N+1); clusters more than one step behind
  take multiple rolls to catch up; three patched lines means up to three release
  branches receiving backports.
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
- **Major rolls from *any* minor of the previous major.** Rejected: every from-minor
  becomes its own upgrade path to test and its own place for latent issues; the
  gateway pins the cross-major surface to one pair, and reaching the gateway is an
  ordinary compatible roll.
- **Postgres-style skip-major offline migration.** Rejected: an offline tool is the
  wrong shape for a clustered broker whose whole point is rolling everything.
- **CalVer.** Rejected: dates say when, not whether an upgrade is safe; semver's
  break/compatible signal is the one operators act on.
