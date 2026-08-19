# ADR 0068 — A FIPS 140-3 cryptographic option

- **Status:** Proposed
- **Date:** 2026-08-19
- **Delivery:** [docs/delivery/0068-fips-cryptographic-option.md](../delivery/0068-fips-cryptographic-option.md)
- **Related:** [ADR 0053](0053-single-crypto-provider-aws-lc-rs.md) (the single-provider
  seam this rides), [ADR 0045](0045-release-engineering-and-distribution.md) (the
  pipeline that would cut the variant), [ADR 0066](0066-threat-model-and-hardening-baseline.md)
  (the hardening baseline the crypto policy joins)

> This record states the decision only. Progress lives in the delivery doc.

## Context

US federal, defense, healthcare, and much of finance cannot deploy software whose
cryptography is not a FIPS 140-validated module. For those buyers this is not a
preference but a gate: no FIPS option, no evaluation.

ADR 0053 already did the hard part without aiming at it: every cryptographic
operation in the workspace rides **aws-lc-rs** — rustls's provider is
`aws_lc_rs::default_provider()` behind one seam (`mqtt-net/src/tls.rs::provider`),
and the direct digest/HMAC call sites (backup integrity, HTTP auth, gossip HMAC) use
the same crate. AWS-LC ships a FIPS 140-3-validated module, exposed through
aws-lc-rs's `fips` feature and rustls's matching FIPS configuration. The distance
from here to a FIPS variant is a feature flag and the discipline around it — for most
brokers it would be a rewrite.

## Decision

A **build variant**, not a default: `mqttd` gains a `fips` cargo feature that

1. switches the workspace `aws-lc-rs` dependency to its `fips` feature (the
   FIPS-validated AWS-LC module; brings a heavier build — cmake/go toolchain — and a
   narrower platform matrix, which is why it is a variant);
2. constructs the rustls provider in FIPS mode and **refuses non-approved
   configuration at startup** (`require_fips`-style assertion): a fips build that
   silently negotiated a non-approved suite would be worse than no fips build;
3. is **visible at runtime**: the version line, the startup log, and a metric label
   state the crypto module and mode, so an auditor can verify the running binary
   rather than trust the download name;
4. ships from the release pipeline as its own artifact set (binary + image, signed,
   SBOM'd, provenance-attested like every other artifact), and is **built and tested
   in CI** — a per-PR compile lane and a nightly lane running the protocol suites
   against the fips binary, because an untested variant is a liability with a
   certificate on it.

A `docs/compliance/crypto-policy.md` states what the variant means precisely: the
*module* is validated (AWS-LC's certificate, referenced by number), the product is
not itself certified, which algorithms are in the approved set, and what an operator
must still configure (the ADR 0066 hardening baseline's TLS items) for a compliant
deployment. Overclaiming here is the industry's standard failure; the document's job
is to make the true claim quotable.

## Consequences

- Two binaries mean a test-matrix cost; the nightly fips lane is the honest floor,
  and the per-PR cost is one compile lane.
- The fips build's platform matrix is narrower than the standard musl set (AWS-LC's
  FIPS module has its own supported-platform list); SUPPORT.md states the difference.
- Algorithm agility narrows in fips mode by design (approved suites only). Features
  that would need non-approved primitives must state their fips behaviour from now
  on — the variant becomes part of the ADR 0039 compatibility surface.

## Tasks

| id | title |
|----|-------|
| 0068-T1 | Spike: workspace `fips` feature building on the supported platforms; record the toolchain and platform-matrix cost |
| 0068-T2 | The fips provider seam: FIPS-mode rustls provider, startup refusal of non-approved config, runtime visibility (version line, log, metric) |
| 0068-T3 | CI lanes: per-PR fips compile; nightly protocol suites against the fips binary |
| 0068-T4 | Release-pipeline fips artifact set (signed, SBOM, provenance) + docs/compliance/crypto-policy.md with the exact validated-module claim |
