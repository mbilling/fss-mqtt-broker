# ADR 0045 — Release engineering and distribution (signed, reproducible, SBOM-attested)

- **Status:** Proposed
- **Date:** 2026-07-17
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0045-release-engineering-and-distribution.md](../delivery/0045-release-engineering-and-distribution.md) — plan, progress, and changelog
- **Related:** [ADR 0039](0039-versioning-and-upgrade-policy.md) (the semver + adjacent-skew
  policy a release enacts — this ADR is how a version number becomes an artifact),
  [ADR 0044](0044-release-readiness-assurance.md) (the readiness checklist whose last open
  box is "cut a signed release"), [ADR 0038](0038-prerelease-compatibility-freeze.md) (the
  pre-release freeze this ends: the first tag is the first frozen wire/schema), [ADR 0024](0024-deterministic-testing.md)
  (the determinism discipline reproducible builds extend to the artifact), [ADR 0002](0002-transport-security.md)
  (the same supply-chain rigor the broker demands of its runtime now applied to its own bytes)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0045-release-engineering-and-distribution.md).

## Context

The engineering is far ahead of the distribution: the broker is tested to a release-grade
bar (ADR 0044), but there is **no release**. No tag, no binary, no image — nothing an
operator can `docker run` or a distro can package. An external review put it plainly: "no
releases = nobody will try it," and named a signed, SBOM-attached release as the single
most on-brand next step for a security-first project.

Two things make this more than a checkbox:

- **The security thesis raises the bar on our own supply chain.** A broker that demands
  mTLS, deny-by-default authz, and a tamper-evident audit log cannot ship unsigned
  binaries from an opaque build. The artifacts must be *signed*, their provenance
  *attestable*, and their contents *enumerable* (an SBOM) — or the pitch is hypocritical.
- **The first tag is a compatibility commitment.** Until now ADR 0038's pre-release freeze
  let wire and schema change freely (wipe-and-rejoin on bumps). The first release is the
  first version another version must interoperate with under ADR 0039; the release process
  is where that line is drawn deliberately, not by accident.

Today there is also no reproducibility story (two builds of the same commit may differ
byte-for-byte), no container image, and no release automation — the tag, if cut by hand,
would be un-auditable.

## Decision

Releasing becomes a **first-class, automated, security-grade pipeline**, gated on the
ADR 0044 checklist. Five parts:

### 1. Semantic version tags drive everything

A release is a signed, annotated git tag `vMAJOR.MINOR.PATCH` following ADR 0039. The tag
is the single input: the pipeline builds, signs, and publishes from it and nothing else. A
`0.x` line ships first (pre-1.0 semantics: breaking changes allowed on MINOR), and the tag
that becomes `1.0.0` is the one that freezes the wire and schema per ADR 0038 — a
conscious, reviewed act, not a drift.

### 2. Reproducible, multi-platform binaries

Builds are **reproducible** — pinned toolchain, vendored/locked dependencies
(`Cargo.lock` committed, already true), stripped of non-deterministic inputs (build paths,
timestamps) — so anyone can rebuild the tag and get the same bytes. Binaries ship for the
platforms operators actually run (at minimum `linux/amd64` and `linux/arm64`; more as
demand appears), each with a checksum.

### 3. Signed artifacts with attestable provenance

Every artifact — binary, checksum file, container image, SBOM — is **signed**, and each
carries **build provenance** (what commit, what workflow, what inputs produced it). Signing
uses keyless OIDC signing (sigstore/cosign) so there is no long-lived key to leak, with the
transparency-log entry as public evidence. Verifying a download is one documented command.

### 4. A minimal, hardened container image

A container image (OCI) built **from scratch / distroless** — no shell, no package
manager, no OS cruft — running the broker as a non-root user, is published to a public
registry per tag and as a moving `latest` for the newest stable. Small attack surface is
the point: the image contains the broker and its CA bundle, nothing else.

### 5. An SBOM per release, and a documented verify path

Each release publishes a **Software Bill of Materials** (a standard format — CycloneDX or
SPDX) enumerating every dependency and version, attached to the release and to the image.
The `cargo-deny`/`cargo-audit` gates (already in CI) run against the release commit, so the
SBOM ships with a clean supply-chain audit. The README and a `RELEASING.md` document the
**verify** path end to end: check the signature, check the checksum, read the SBOM.

## Consequences

- The ADR 0044 release-readiness checklist's last open box ("signed release") closes, and
  0039-T3's adjacent-skew CI test gains its first pair of real versions to run against.
- "Nobody can try it" ends: `docker run` and a downloadable binary exist, both verifiable.
- The security posture extends to the project's own bytes — signing, provenance, SBOM — so
  "most secure" covers the supply chain, not only the runtime.
- The first tag is a real commitment: after it, ADR 0038's free-reshape era is over and
  ADR 0039's skew rules bind. Cutting `1.0.0` specifically is therefore a deliberate,
  reviewed decision, not an increment.
- CI cost grows only on tag (the release workflow is separate from per-PR CI); reproducible
  builds add discipline (no `SystemTime::now()` in build scripts, etc.) that is cheap to
  keep once established.

## Alternatives considered

- **Hand-cut releases (upload a binary to a GitHub release by hand):** un-auditable,
  unsigned, unreproducible, and exactly the opacity a security-branded project must not
  ship. Rejected.
- **A long-lived signing key (GPG):** works, but a key to store, rotate, and leak — the
  keyless OIDC/transparency-log model removes the secret entirely and produces public
  evidence, a better fit for the threat model.
- **A fat container image (Debian/Alpine base):** convenient, but ships a shell, a package
  manager, and dozens of CVE-bearing OS packages irrelevant to the broker. Distroless/scratch
  costs a little build friction for a much smaller attack surface — the right trade here.
- **Deferring releases until 1.0:** the analysis's point stands — a project no one can run
  gets no users, no feedback, and no adoption signal. A `0.x` line ships now; 1.0 is the
  compatibility commitment, not the first artifact.
- **Publishing to crates.io as the primary channel:** the broker is an application, not a
  library; a signed binary + image is what operators consume. The workspace crates can be
  published later if there is demand to embed them.
