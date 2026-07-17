---
adr: "0045"
title: "Release engineering and distribution (signed, reproducible, SBOM-attested)"
adr_status: Proposed
tasks:
  - id: 0045-T1
    title: Release CI pipeline triggered by a signed semver tag — builds from the tag alone, nothing else; produces the artifact set (binaries, checksums, image, SBOM) as workflow outputs
    status: planned
  - id: 0045-T2
    title: Reproducible multi-platform binaries — pinned toolchain, committed lockfile, stripped of build paths/timestamps; linux/amd64 + linux/arm64 with per-artifact checksums; a rebuild-the-tag check proves byte-identity
    status: planned
  - id: 0045-T3
    title: Keyless signing + provenance — cosign/sigstore signatures on every artifact and image, build-provenance attestation, transparency-log entry; a one-command documented verify path
    status: planned
  - id: 0045-T4
    title: Hardened container image — distroless/scratch, non-root, broker + CA bundle only; published per tag and as latest for newest stable
    status: planned
  - id: 0045-T5
    title: SBOM per release (CycloneDX or SPDX) attached to the release and image; cargo-deny/cargo-audit run on the release commit; RELEASING.md + README verify docs; cut the first 0.x release
    status: planned
---

# Delivery: ADR 0045 — Release engineering and distribution

[ADR 0045](../adr/0045-release-engineering-and-distribution.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0045 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0045-T1 | ⬜ planned | — |  |
| 0045-T2 | ⬜ planned | — |  |
| 0045-T3 | ⬜ planned | — |  |
| 0045-T4 | ⬜ planned | — |  |
| 0045-T5 | ⬜ planned | — |  |
<!-- /status-table:0045 -->

## Plan

| Task | Done means |
|---|---|
| **0045-T1** Release pipeline | A signed `vX.Y.Z` tag triggers a workflow that builds, signs, and publishes from the tag alone; per-PR CI is untouched. |
| **0045-T2** Reproducible binaries | `linux/amd64` + `linux/arm64` binaries with checksums; rebuilding the tag yields byte-identical output (proven by a CI rebuild-and-compare). |
| **0045-T3** Signing + provenance | Every artifact and image is cosign-signed with build provenance and a transparency-log entry; `README`/`RELEASING.md` document a one-command verify. |
| **0045-T4** Container image | A distroless/scratch, non-root image is published per tag and as `latest`; it contains the broker and CA bundle, nothing else. |
| **0045-T5** SBOM + first release | A CycloneDX/SPDX SBOM ships per release with a clean supply-chain audit; the first `0.x` release is cut end to end. |

Order: T1 → T2/T3/T4 (parallel on the pipeline) → T5 (SBOM + the actual cut). T5's release
is gated on the ADR 0044 readiness checklist being green.

## Changelog

- **2026-07-17** — ADR 0045 drafted. The single ship-blocking gap (no release exists);
  closes the last open box on the ADR 0044 release-readiness checklist and gives 0039-T3
  its first real version pair. Priority **P0**.
