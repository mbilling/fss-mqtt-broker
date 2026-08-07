---
adr: "0045"
title: "Release engineering and distribution (signed, reproducible, SBOM-attested)"
adr_status: Proposed
tasks:
  - id: 0045-T1
    title: Release CI pipeline triggered by a signed semver tag — builds from the tag alone, nothing else; produces the artifact set (binaries, checksums, image, SBOM) as workflow outputs
    status: done
    date: 2026-07-18
    evidence: ".github/workflows/release.yml — triggers only on v*.*.* tags; audit→build(matrix)→release graph builds/signs/publishes from the tag; per-PR ci.yml untouched"
  - id: 0045-T2
    title: Reproducible multi-platform binaries — pinned toolchain, committed lockfile, stripped of build paths/timestamps; linux/amd64 + linux/arm64 with per-artifact checksums; a rebuild-the-tag check proves byte-identity
    status: done
    date: 2026-07-18
    evidence: "rust-toolchain.toml pins 1.97.0; scripts/release/build-repro.sh (locked, path-remap, SOURCE_DATE_EPOCH, crt-static); fully-static musl amd64+arm64 built natively; rebuilt byte-identical locally (sha 9a2e2f3…); pipeline re-checks byte-identity every release"
  - id: 0045-T3
    title: Keyless signing + provenance — cosign/sigstore signatures on every artifact and image, build-provenance attestation, transparency-log entry; a one-command documented verify path
    status: done
    date: 2026-08-07
    evidence: "The remaining condition — 'first real signatures/Rekor entries are produced by the first tag run' — was met by v0.9.0-rc on 2026-07-22, and the documented verify path has now been EXECUTED against those real published artifacts with cosign 3.1.3 rather than only written down. Verified end to end: binary signature (cosign verify-blob on mqttd-0.9.0-rc-x86_64-unknown-linux-musl with its .pem/.sig and the real workflow identity + GitHub OIDC issuer) -> 'Verified OK'; sha256 checksum -> OK; SBOM blob signature -> 'Verified OK' (CycloneDX 1.3, 276 components); container image signature on ghcr.io/mbilling/fss-mqtt-broker:0.9.0-rc -> cosign claims validated, transparency-log entry verified offline, code-signing cert verified against the trusted CA; SLSA provenance attestation -> OK; CycloneDX attestation on the image -> predicateType https://cyclonedx.org/bom, 276 components. THREE DEFECTS IN THE RUNBOOK, all findable only by running it: (1) step 3 documented `--type slsaprovenance`, which FAILS — the bare name is cosign's alias for SLSA v0.2 while the pipeline emits v1 ('none of the attestations matched the predicate type'); corrected to slsaprovenance1, and the reason recorded inline. (2) step 4 documented `cosign download sbom`, which is deprecated, prints nothing, and per cosign's own warning would not authenticate the SBOM anyway — and it was written with a trailing `|| true`, turning the failure into a silent success; replaced with the two paths that work and authenticate (verify-blob on the release asset, or verify-attestation on the image). (3) cosign 3.x deprecates --certificate/--signature in favour of --bundle; they still verify, and the runbook now says so instead of leaving a user wondering. The one-command verify path is now proven, not asserted."
  - id: 0045-T4
    title: Hardened container image — distroless/scratch, non-root, broker + CA bundle only; published per tag and as latest for newest stable
    status: done
    date: 2026-07-18
    evidence: "Dockerfile → gcr.io/distroless/static-debian12:nonroot with a fully-static musl binary (no libc); built + ran non-root (uid 65532) in-container locally, 27.4MB, stayed Up — fixes the glibc-skew crash a distroless/cc glibc build hit"
  - id: 0045-T5
    title: SBOM per release (CycloneDX or SPDX) attached to the release and image; cargo-deny/cargo-audit run on the release commit; RELEASING.md + README verify docs; cut the first 0.x release
    status: in-progress
    date: 2026-07-18
    notes: "CycloneDX SBOM (cargo-cyclonedx) + cargo-deny/cargo-audit gate on the release commit + RELEASING.md + README Install/verify — all in place; remaining: cut the first 0.x release (a maintainer signed-tag push, gated on the ADR 0044 readiness checklist)"
---

# Delivery: ADR 0045 — Release engineering and distribution

[ADR 0045](../adr/0045-release-engineering-and-distribution.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0045 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0045-T1 | ✅ done | 2026-07-18 | ".github/workflows/release.yml — triggers only on v*.*.* tags; audit→build(matrix)→release graph builds/signs/publishes from the tag; per-PR ci.yml untouched" |
| 0045-T2 | ✅ done | 2026-07-18 | "rust-toolchain.toml pins 1.97.0; scripts/release/build-repro.sh (locked, path-remap, SOURCE_DATE_EPOCH, crt-static); fully-static musl amd64+arm64 built natively; rebuilt byte-identical locally (sha 9a2e2f3…); pipeline re-checks byte-identity every release" |
| 0045-T3 | ✅ done | 2026-08-07 | "The remaining condition — 'first real signatures/Rekor entries are produced by the first tag run' — was met by v0.9.0-rc on 2026-07-22, and the documented verify path has now been EXECUTED against those real published artifacts with cosign 3.1.3 rather than only written down. Verified end to end: binary signature (cosign verify-blob on mqttd-0.9.0-rc-x86_64-unknown-linux-musl with its .pem/.sig and the real workflow identity + GitHub OIDC issuer) -> 'Verified OK'; sha256 checksum -> OK; SBOM blob signature -> 'Verified OK' (CycloneDX 1.3, 276 components); container image signature on ghcr.io/mbilling/fss-mqtt-broker:0.9.0-rc -> cosign claims validated, transparency-log entry verified offline, code-signing cert verified against the trusted CA; SLSA provenance attestation -> OK; CycloneDX attestation on the image -> predicateType https://cyclonedx.org/bom, 276 components. THREE DEFECTS IN THE RUNBOOK, all findable only by running it: (1) step 3 documented `--type slsaprovenance`, which FAILS — the bare name is cosign's alias for SLSA v0.2 while the pipeline emits v1 ('none of the attestations matched the predicate type'); corrected to slsaprovenance1, and the reason recorded inline. (2) step 4 documented `cosign download sbom`, which is deprecated, prints nothing, and per cosign's own warning would not authenticate the SBOM anyway — and it was written with a trailing `|| true`, turning the failure into a silent success; replaced with the two paths that work and authenticate (verify-blob on the release asset, or verify-attestation on the image). (3) cosign 3.x deprecates --certificate/--signature in favour of --bundle; they still verify, and the runbook now says so instead of leaving a user wondering. The one-command verify path is now proven, not asserted." |
| 0045-T4 | ✅ done | 2026-07-18 | "Dockerfile → gcr.io/distroless/static-debian12:nonroot with a fully-static musl binary (no libc); built + ran non-root (uid 65532) in-container locally, 27.4MB, stayed Up — fixes the glibc-skew crash a distroless/cc glibc build hit" |
| 0045-T5 | 🚧 in-progress | 2026-07-18 | "CycloneDX SBOM (cargo-cyclonedx) + cargo-deny/cargo-audit gate on the release commit + RELEASING.md + README Install/verify — all in place; remaining: cut the first 0.x release (a maintainer signed-tag push, gated on the ADR 0044 readiness checklist)" |
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
- **2026-07-18** — Pipeline built (T1, T2, T4 done; T3, T5 in-progress pending the first
  tag run). `.github/workflows/release.yml` (audit → build matrix → sign+publish),
  `scripts/release/build-repro.sh`, `rust-toolchain.toml` (pin 1.97.0), `Dockerfile`,
  `RELEASING.md`, and a README **Install** section. Two design calls proven on hardware
  before committing them:
  - **Reproducibility is real, not aspirational:** rebuilding the same commit is
    byte-identical (`sha 9a2e2f3…`), via the locked-toolchain + path-remap +
    `SOURCE_DATE_EPOCH` recipe in `build-repro.sh`.
  - **Fully-static musl over glibc/distroless-cc:** a first cut built a glibc binary for
    `distroless/cc`, which crash-looped in the image (`GLIBC_2.38 not found` — the build
    host's newer glibc leaked in). Switching to static-musl on `distroless/static`
    eliminates the libc-skew *and* the reproducibility hole (no glibc bytes leak), and
    shrinks the attack surface further (no dynamic loader). The static image builds, runs
    non-root, and stays up (27.4MB).
  Also fixed the workspace `repository` placeholder (`TODO/mqtt-broker` →
  `mbilling/fss-mqtt-broker`). Remaining for closure: the first signed `v0.x` tag, which
  exercises signing/provenance/SBOM end to end and flips the ADR to Accepted.
