---
adr: "0068"
title: "A FIPS 140-3 cryptographic option"
adr_status: Proposed
tasks:
  - id: 0068-T1
    title: "Spike: workspace `fips` feature building on the supported platforms; record the toolchain and platform-matrix cost"
    status: planned
  - id: 0068-T2
    title: "The fips provider seam: FIPS-mode rustls provider, startup refusal of non-approved config, runtime visibility (version line, log, metric)"
    status: planned
  - id: 0068-T3
    title: "CI lanes: per-PR fips compile; nightly protocol suites against the fips binary"
    status: planned
  - id: 0068-T4
    title: "Release-pipeline fips artifact set (signed, SBOM, provenance) + docs/compliance/crypto-policy.md with the exact validated-module claim"
    status: planned
---

# Delivery — ADR 0068: A FIPS 140-3 cryptographic option

Decision: [docs/adr/0068-fips-cryptographic-option.md](../adr/0068-fips-cryptographic-option.md).

ADR 0053's single-provider consolidation (everything on aws-lc-rs, one rustls
provider seam) is what makes this a feature flag plus discipline instead of a
rewrite. The claim shipped is the module's validation, never the product's — the
crypto-policy document exists to make the true claim quotable and the overclaim
impossible.

## Progress

<!-- status-table:0068 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0068-T1 | ⬜ planned | — |  |
| 0068-T2 | ⬜ planned | — |  |
| 0068-T3 | ⬜ planned | — |  |
| 0068-T4 | ⬜ planned | — |  |
<!-- /status-table:0068 -->

## Changelog

- **2026-08-19** — ADR proposed and delivery opened, from the post-1.0-freeze
  enterprise-readiness review: the single highest-leverage product feature on the
  enterprise list, and the one ADR 0053 made cheap.
