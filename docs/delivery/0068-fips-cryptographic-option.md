---
adr: "0068"
title: "A FIPS 140-3 cryptographic option"
adr_status: Accepted
tasks:
  - id: 0068-T1
    title: "Spike: workspace `fips` feature building on the supported platforms; record the toolchain and platform-matrix cost"
    status: done
    date: 2026-08-19
    evidence: "The spike outcome, better than the ADR hoped: the whole graph COMPILES IN FIPS MODE with a feature flag — cargo check -p mqttd --features fips green on macOS arm64 and in the new CI lane on linux — because ADR 0053's one-provider consolidation left a single seam. Feature wiring: each crypto-touching crate (mqtt-net, mqtt-auth, mqtt-cluster, mqtt-observability, mqttd) declares fips forwarding to aws-lc-rs/fips, plus rustls/fips, tokio-rustls/fips and quinn/rustls-aws-lc-rs-fips in mqtt-net; cargo feature unification flips the entire graph including rustls's internal usage. Toolchain cost recorded: cmake + Go (aws-lc-fips-sys 0.14.1 halts loudly without cmake — reproduced, then installed). Platform matrix recorded honestly in docs/compliance/crypto-policy.md: validated platforms are Linux; macOS compiles for development; the musl-static release targets need their own spike (deferred into T4's notes)."
  - id: 0068-T2
    title: "The fips provider seam: FIPS-mode rustls provider, startup refusal of non-approved config, runtime visibility (version line, log, metric)"
    status: done
    date: 2026-08-19
    evidence: "The seam, at ADR 0002's single audited build site: provider() asserts p.fips() in fips builds — a fips binary that somehow ran non-approved crypto aborts rather than serves; server_config_versions refuses the TLS 1.2 opt-in at startup naming the reason (the variant serves the approved posture only, which also closes the unsafe-features hatch by construction). Runtime visibility, all three of the ADR's channels: the startup banner logs crypto = crypto_module() ('aws-lc-rs (FIPS mode)' vs 'aws-lc-rs'), /statusz carries a crypto field beside version, and crypto_module_info{module} 1 exports it as a metric — so an auditor verifies the RUNNING binary, not the artifact name. Pinned by fips_posture_tests (run under --features fips): the provider IS in FIPS mode, ChaCha20 suites are absent, the 1.2 opt-in is refused while the approved posture builds; the standard build's tls12_hardening_tests are gated to not(fips) since the variant deliberately changes that posture (X25519 and ChaCha20 vanish — observed, which is what distinguishes the validated module). Standard suite unchanged: mqtt-net 21/21, mqttd lib 330/330."
  - id: 0068-T3
    title: "CI lanes: per-PR fips compile; nightly protocol suites against the fips binary"
    status: done
    date: 2026-08-19
    evidence: "Per-PR: the ci.yml fips job (45-min leash) compiles the whole graph with --all-targets in fips mode and runs the posture tests — a change that breaks the variant is caught at review, not at certification time. Nightly: the fips-protocol job builds the fips binary and runs the real client-visible suites against it (v5_protocol, tls, binary_smoke — the last covering the process-level boot/shutdown/audit-export story), because identical protocol behaviour under the validated module is proven, not asserted. Both lanes use the runner image's preinstalled cmake + Go; both carry hard timeouts per the ci-leash rule."
  - id: 0068-T4
    title: "Release-pipeline fips artifact set (signed, SBOM, provenance) + docs/compliance/crypto-policy.md with the exact validated-module claim"
    status: done
    date: 2026-08-20
    evidence: "The musl spike answered YES on both release targets, in three rounds each teaching something: round 1 — aws-lc-fips-sys's urandom.c includes Linux kernel UAPI headers that Debian's musl-gcc keeps off its path by design; fixed with -idirafter (musl's headers always win, only the kernel interfaces resolve from distro locations), now part of the release recipe. Round 2 compiled the whole graph clean (the failure was a cp into a directory only the standard build creates — mkdir -p). Round 3: build + static-binary execution (--version) + BYTE-IDENTICAL clean rebuild, green on x86_64 and aarch64 musl. WIRED: build-repro.sh gains the mqttd-fips token (isolated target dir under target/ so the feature build can never overwrite the standard binary; covered by the repro proof's cargo clean); release.yml builds/checksums/uploads mqttd-fips-<version>-<target> through the same staging, includes it in the clean-rebuild reproducibility proof and the SLSA provenance attestation, and the existing cosign loop signs it; a fips-module-<version>.txt asset pins the exact aws-lc-fips-sys version from the tagged lockfile and points at the module's authoritative CMVP documentation — the certificate claim is emitted at build time, never quoted from memory. Deliberate scope line: a binary artifact only — a fips container image is its own future decision. crypto-policy.md updated from 'needs its own spike' to the proven platform statement. The spike workflow is retired; the nightly fips-protocol lane remains the ongoing assurance."
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
| 0068-T1 | ✅ done | 2026-08-19 | "The spike outcome, better than the ADR hoped: the whole graph COMPILES IN FIPS MODE with a feature flag — cargo check -p mqttd --features fips green on macOS arm64 and in the new CI lane on linux — because ADR 0053's one-provider consolidation left a single seam. Feature wiring: each crypto-touching crate (mqtt-net, mqtt-auth, mqtt-cluster, mqtt-observability, mqttd) declares fips forwarding to aws-lc-rs/fips, plus rustls/fips, tokio-rustls/fips and quinn/rustls-aws-lc-rs-fips in mqtt-net; cargo feature unification flips the entire graph including rustls's internal usage. Toolchain cost recorded: cmake + Go (aws-lc-fips-sys 0.14.1 halts loudly without cmake — reproduced, then installed). Platform matrix recorded honestly in docs/compliance/crypto-policy.md: validated platforms are Linux; macOS compiles for development; the musl-static release targets need their own spike (deferred into T4's notes)." |
| 0068-T2 | ✅ done | 2026-08-19 | "The seam, at ADR 0002's single audited build site: provider() asserts p.fips() in fips builds — a fips binary that somehow ran non-approved crypto aborts rather than serves; server_config_versions refuses the TLS 1.2 opt-in at startup naming the reason (the variant serves the approved posture only, which also closes the unsafe-features hatch by construction). Runtime visibility, all three of the ADR's channels: the startup banner logs crypto = crypto_module() ('aws-lc-rs (FIPS mode)' vs 'aws-lc-rs'), /statusz carries a crypto field beside version, and crypto_module_info{module} 1 exports it as a metric — so an auditor verifies the RUNNING binary, not the artifact name. Pinned by fips_posture_tests (run under --features fips): the provider IS in FIPS mode, ChaCha20 suites are absent, the 1.2 opt-in is refused while the approved posture builds; the standard build's tls12_hardening_tests are gated to not(fips) since the variant deliberately changes that posture (X25519 and ChaCha20 vanish — observed, which is what distinguishes the validated module). Standard suite unchanged: mqtt-net 21/21, mqttd lib 330/330." |
| 0068-T3 | ✅ done | 2026-08-19 | "Per-PR: the ci.yml fips job (45-min leash) compiles the whole graph with --all-targets in fips mode and runs the posture tests — a change that breaks the variant is caught at review, not at certification time. Nightly: the fips-protocol job builds the fips binary and runs the real client-visible suites against it (v5_protocol, tls, binary_smoke — the last covering the process-level boot/shutdown/audit-export story), because identical protocol behaviour under the validated module is proven, not asserted. Both lanes use the runner image's preinstalled cmake + Go; both carry hard timeouts per the ci-leash rule." |
| 0068-T4 | ✅ done | 2026-08-20 | "The musl spike answered YES on both release targets, in three rounds each teaching something: round 1 — aws-lc-fips-sys's urandom.c includes Linux kernel UAPI headers that Debian's musl-gcc keeps off its path by design; fixed with -idirafter (musl's headers always win, only the kernel interfaces resolve from distro locations), now part of the release recipe. Round 2 compiled the whole graph clean (the failure was a cp into a directory only the standard build creates — mkdir -p). Round 3: build + static-binary execution (--version) + BYTE-IDENTICAL clean rebuild, green on x86_64 and aarch64 musl. WIRED: build-repro.sh gains the mqttd-fips token (isolated target dir under target/ so the feature build can never overwrite the standard binary; covered by the repro proof's cargo clean); release.yml builds/checksums/uploads mqttd-fips-<version>-<target> through the same staging, includes it in the clean-rebuild reproducibility proof and the SLSA provenance attestation, and the existing cosign loop signs it; a fips-module-<version>.txt asset pins the exact aws-lc-fips-sys version from the tagged lockfile and points at the module's authoritative CMVP documentation — the certificate claim is emitted at build time, never quoted from memory. Deliberate scope line: a binary artifact only — a fips container image is its own future decision. crypto-policy.md updated from 'needs its own spike' to the proven platform statement. The spike workflow is retired; the nightly fips-protocol lane remains the ongoing assurance." |
<!-- /status-table:0068 -->

## Changelog

- **2026-08-20** — T4 done: the musl spike passed on both release targets
  (three rounds; kernel-header shim now in the release recipe) and the fips
  artifact set is wired with the full repro/sign/provenance treatment.
  ADR 0068 complete (4/4); status Accepted.

- **2026-08-19** — T1-T3 shipped in one pass (the spike came back cleaner than
  the ADR hoped: one feature flag flips the whole graph); T4's policy document
  ships, its artifact half gated on the musl-fips spike, recorded in notes.

- **2026-08-19** — ADR proposed and delivery opened, from the post-1.0-freeze
  enterprise-readiness review: the single highest-leverage product feature on the
  enterprise list, and the one ADR 0053 made cheap.
